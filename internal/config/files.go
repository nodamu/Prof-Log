package config

import (
	"os"
	"path/filepath"
)

var (
	CAFile               = configFile("ca.pem")
	ServerCertFile       = configFile("server.pem")
	ServerKeyFile        = configFile("server-key.pem")
	ClientCertFile       = configFile("client.pem")
	ClientKeyFile        = configFile("client-key.pem")
	RootClientCertFile   = configFile("root-client.pem")
	RootClientKeyFile    = configFile("root-client-key.pem")
	NobodyClientCertFile = configFile("nobody-client.pem")
	NobodyClientKeyFile  = configFile("nobody-client-key.pem")
	ACLModelFile         = configFile("model.conf")
	ACLPolicyFile        = configFile("policy.csv")
)

// configFile gets the path to the certs
func configFile(filename string) string {
	if dir := os.Getenv("CONFIG_PATH"); dir != " " {
		//fmt.Printf("Config path: %s \n", filepath.Join(dir, filename))
		return filepath.Join(dir, filename)
	}

	homeDir, err := os.UserHomeDir()

	//fmt.Printf("Home dir %s \n", homeDir)
	if err != nil {
		panic(err)
	}

	return filepath.Join(homeDir, ".proglog", filename)
}
