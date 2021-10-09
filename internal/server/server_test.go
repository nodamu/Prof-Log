package server

import (
	"context"
	api "github.com/nodamu/prof-log/api/v1"
	"github.com/nodamu/prof-log/internal/auth"
	"github.com/nodamu/prof-log/internal/config"
	"github.com/nodamu/prof-log/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"io/ioutil"
	"net"
	"testing"
)

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		rootClient api.LogClient,
		nobodyClient api.LogClient,
		//config *Config
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
		"unauthorized fails":                                 testUnauthorized,
	} {
		t.Run(scenario, func(t *testing.T) {
			rootClient, nobodyClient, _, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, rootClient, nobodyClient)
		})
	}
}

func testConsumePastBoundary(t *testing.T, client api.LogClient, _ api.LogClient) {
	ctx := context.Background()

	msg := &api.Record{Value: []byte("Chicken Wings")}

	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: msg})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset + 1})

	if consume != nil {
		t.Fatalf("Expected a nil record")
	}

	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())

	if got != want {
		t.Fatalf("Expected %v, got %v", got, want)
	}

}

func testProduceConsumeStream(t *testing.T, client api.LogClient, _ api.LogClient) {
	ctx := context.Background()

	records := []*api.Record{{
		Value:  []byte("Chicken Wings"),
		Offset: 0,
	}, {
		Value:  []byte("Gob3"),
		Offset: 1,
	},
	}
	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)
		for offset, record := range records {
			err := stream.Send(&api.ProduceRequest{Record: record})
			require.NoError(t, err)
			res, err := stream.Recv()
			require.NoError(t, err)

			if res.Offset != uint64(offset) {
				t.Fatalf("Expected: %d, got %d", offset, res.Offset)
			}

		}
	}
	{
		stream, err := client.ConsumeStream(
			ctx,
			&api.ConsumeRequest{Offset: 0},
		)
		require.NoError(t, err)

		for i, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Record, &api.Record{
				Value:  record.Value,
				Offset: uint64(i),
			})
		}

	}

}

func testUnauthorized(t *testing.T, _, client api.LogClient) {
	ctx := context.Background()

	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("Chicken Wings"),
		},
	})
	if produce != nil {
		t.Fatalf("Produce offset should be nil")
	}

	got, want := status.Code(err), codes.PermissionDenied

	if got != want {
		t.Fatalf("Expected %s but got %s", want, got)
	}

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: 0})

	if consume != nil {
		t.Fatalf("Consume offset should be nil")
	}

	got, wantCode := status.Code(err), codes.PermissionDenied

	if got != wantCode {
		t.Fatalf("Expected %s but got %s", want, got)
	}
}

func setupTest(t *testing.T, fn func(config *Config)) (
	rootClient api.LogClient,
	nobodyClient api.LogClient,
	cfg *Config,
	teardown func(),
) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	newClient := func(crtPath, keyPath string) (
		*grpc.ClientConn,
		api.LogClient,
		[]grpc.DialOption,
	) {
		clientTLS, err := config.SetupTLSConfig(config.TLSConfig{
			CAFile:   config.CAFile,
			Keyfile:  keyPath,
			Certfile: crtPath,
			Server:   false,
		})

		require.NoError(t, err)

		clientCreds := credentials.NewTLS(clientTLS)
		opts := []grpc.DialOption{grpc.WithTransportCredentials(clientCreds)}

		//clientOptions := []grpc.DialOption{grpc.WithInsecure()}
		cc, err := grpc.Dial(l.Addr().String(), opts...)
		require.NoError(t, err)
		client := api.NewLogClient(cc)

		return cc, client, opts
	}

	var rootConn *grpc.ClientConn
	rootConn, rootClient, _ = newClient(
		config.RootClientCertFile,
		config.RootClientKeyFile,
	)

	var nobodyConn *grpc.ClientConn
	nobodyConn, nobodyClient, _ = newClient(
		config.NobodyClientCertFile,
		config.NobodyClientKeyFile,
	)

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		Certfile:      config.ServerCertFile,
		CAFile:        config.CAFile,
		Keyfile:       config.ServerKeyFile,
		ServerAddress: l.Addr().String(),
		Server:        true,
	})
	require.NoError(t, err)

	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := ioutil.TempDir("", "server-test")
	require.NoError(t, err)
	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	authorize := auth.New(config.ACLModelFile, config.ACLPolicyFile)

	cfg = &Config{
		CommitLog:  clog,
		Authorizer: authorize,
	}
	if fn != nil {
		fn(cfg)
	}
	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()
	return rootClient, nobodyClient, cfg, func() {
		server.Stop()
		rootConn.Close()
		nobodyConn.Close()
		l.Close()
		clog.Remove()
	}
}

func testProduceConsume(t *testing.T, client api.LogClient, _ api.LogClient) {
	ctx := context.Background()

	want := &api.Record{
		Value: []byte("Chicken wings"),
	}

	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}
