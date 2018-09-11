package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	// The dxda package should contain all core functionality
	"github.com/geetduggal/dxda"
	"github.com/google/subcommands"
)

type downloadCmd struct {
	maxThreads int
}

func (*downloadCmd) Name() string     { return "download" }
func (*downloadCmd) Synopsis() string { return "Download files in a manifest" }
func (*downloadCmd) Usage() string {
	return `download [-max_threads] manifest.json.bz2`
}
func (p *downloadCmd) SetFlags(f *flag.FlagSet) {
	f.IntVar(&p.maxThreads, "max_threads", 8, "Maximum # of threads to use when downloading files")
}

func (p *downloadCmd) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	fname := f.Args()[0]
	token, method := dxda.GetToken()
	fmt.Printf("Obtained token using %s\n", method)
	var opts dxda.Opts
	opts.NumThreads = p.maxThreads
	if _, err := os.Stat(fname + ".stats.db"); os.IsNotExist(err) {
		dxda.CreateManifestDB(fname)
	}
	dxda.DownloadManifestDB(fname, token, opts)
	return subcommands.ExitSuccess
}

type progressCmd struct {
	maxThreads int
}

func (*progressCmd) Name() string     { return "progress" }
func (*progressCmd) Synopsis() string { return "Download files in a manifest" }
func (*progressCmd) Usage() string {
	return `progress manifest.json.bz2`
}
func (p *progressCmd) SetFlags(f *flag.FlagSet) {
}
func (p *progressCmd) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	fname := f.Args()[0]
	fmt.Println(dxda.DownloadProgress(fname))
	return subcommands.ExitSuccess
}

// The CLI is simply a wrapper around the dxda package
func main() {
	subcommands.Register(subcommands.HelpCommand(), "")
	subcommands.Register(subcommands.FlagsCommand(), "")
	subcommands.Register(subcommands.CommandsCommand(), "")
	subcommands.Register(&downloadCmd{}, "")
	subcommands.Register(&progressCmd{}, "")

	flag.Parse()
	ctx := context.Background()
	os.Exit(int(subcommands.Execute(ctx)))
}
