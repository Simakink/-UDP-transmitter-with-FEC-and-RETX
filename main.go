package main

import (
	"github.com/spf13/cobra"
	"streamer/cmd/client"
	"streamer/cmd/server"
)

var rootCmd = &cobra.Command{Use: "streamer"}

func init() {
	rootCmd.AddCommand(server.ServerCmd)
	rootCmd.AddCommand(client.ClientCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		panic(err)
	}
}
