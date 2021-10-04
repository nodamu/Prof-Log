package server

import (
	"context"
	api "github.com/nodamu/prof-log/api/v1"
	"google.golang.org/grpc"
)

type CommitLog interface {
	Append(*api.Record) (uint64,error)
	Read(uint64)(*api.Record,error)
}

type Config struct {
	CommitLog CommitLog
}


// Embedded struct that implements the LogServer interface
type grpcServer struct {
	api.UnimplementedLogServer
	 *Config
}


// newgrpcServer factory method to create a grpcServer object
func newgrpcServer(config *Config)(srv *grpcServer,err error){
	srv = &grpcServer{
		Config: config,
	}

	return srv, nil
}

// NewGRPCServer is a factory method to create a grpc server
func NewGRPCServer(config *Config)(*grpc.Server,error){
	gsrv := grpc.NewServer()
	srv,err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv,srv)

	return gsrv, nil
}

func (s *grpcServer) Produce(ctx context.Context, request *api.ProduceRequest) (*api.ProduceResponse, error) {
	offset, err := s.CommitLog.Append(request.Record)
	if err != nil {
		return nil, err
	}

	return &api.ProduceResponse{Offset: offset},nil
}

func (s *grpcServer) Consume(ctx context.Context, request *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	record, read := s.CommitLog.Read(request.Offset)
	if read != nil {
		return nil, read
	}
	return &api.ConsumeResponse{Record: record,},nil
}

func (s *grpcServer) ConsumeStream(request *api.ConsumeRequest,
	stream api.Log_ConsumeStreamServer) error {
	for{
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(),request)
			switch err.(type) {
			case nil:
			//case api.ErrOffsetOutOfRange:
			//	continue
			default:
				return err
				
			}
			if err = stream.Send(res); err != nil {
				return err
			}
		}
		
		
	}
}

func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for  {
		req,err := stream.Recv()
		if err != nil {
			return err
		}

		res,err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}

		if err = stream.Send(res); err != nil {
			return err
		}
	}
}




