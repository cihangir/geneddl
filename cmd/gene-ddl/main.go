// Package main provides ddl plugin for gene package
package main

import (
	gplugin "github.com/cihangir/gene/plugin"
	"github.com/cihangir/geneddl"
	"github.com/hashicorp/go-plugin"
)

func main() {
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: gplugin.HandshakeConfig,
		Plugins: map[string]plugin.Plugin{
			"generate": gplugin.NewGeneratorPlugin(geneddl.New()),
		},
	})
}
