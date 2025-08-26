package main

import (
	"log/slog"
	"net"
	"os"
	"strconv"

	"github.com/goccy/go-yaml"
	"github.com/openGemini/observability/trace/gen/jaeger_storage/v2"
	"github.com/openGemini/observability/trace/gen/otlp/collector/trace/v1"
	"github.com/openGemini/observability/trace/service"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var (
	rootCmd *cobra.Command
)

func init() {
	var (
		configFile string
		cfg        *service.Config
	)
	rootCmd = &cobra.Command{
		Use:     "ts-trace",
		Short:   "use openGemini as trace(jaeger) storage backend",
		Example: "ts-trace --config=config.yaml",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			var err error
			cfg, err = readConfigFile(configFile)
			if err != nil {
				slog.Error("failed to read config", "file", configFile, "error", err)
				return err
			}
			cfg.String()
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			var address = net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
			listener, err := net.Listen("tcp", address)
			if err != nil {
				slog.Error("listen address failed", "reason", err, "address", address)
				return err
			}
			slog.Info("server starting", "address", address)
			server, err := service.NewOpenGeminiStorage(cfg)
			if err != nil {
				slog.Error("create opengemini storage server failed", "reason", err)
				return err
			}
			var opts []grpc.ServerOption
			grpcServer := grpc.NewServer(opts...)
			jaeger_storage.RegisterTraceReaderServer(grpcServer, server)
			jaeger_storage.RegisterDependencyReaderServer(grpcServer, server)
			collector_trace.RegisterTraceServiceServer(grpcServer, server)
			if err := grpcServer.Serve(listener); err != nil {
				slog.Error("serve grpc server failed", "reason", err)
				return err
			}
			return nil
		},
		PostRunE: nil,
	}

	rootCmd.Flags().StringVarP(&configFile, "config", "c", "config.yaml", "specify service running configuration")
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		slog.Error("execute command failed", "reason", err)
		return
	}
}

func readConfigFile(filename string) (*service.Config, error) {
	content, err := os.ReadFile(filename)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if os.IsNotExist(err) {
		return &service.Config{
			Host: service.DefaultHost,
			Port: service.DefaultPort,
			OpenGeminiConfig: &service.OpenGeminiConfig{
				Address: []*service.OpenGeminiConfigAddress{
					{
						Host: service.DefaultOpenGeminiHost,
						Port: service.DefaultOpenGeminiPort,
					},
				},
				Auth:              nil,
				Database:          service.DefaultDatabase,
				RetentionPolicy:   service.DefaultRetentionPolicy,
				RetentionDuration: service.DefaultRetentionDuration,
				GrpcWrite: &service.OpenGeminiGrpcWrite{Address: []*service.OpenGeminiConfigAddress{
					{
						Host: service.DefaultOpenGeminiHost,
						Port: service.DefaultOpenGeminiGrpcPort,
					},
				}},
			},
		}, nil
	}
	var cfg = new(service.Config)
	err = yaml.Unmarshal(content, cfg)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}
