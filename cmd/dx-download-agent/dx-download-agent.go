package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	// The dxda package should contain all core functionality
	"github.com/dnanexus/dxda"
	"github.com/google/subcommands"
)

type downloadCmd struct {
	maxThreads int
}

const downloadUsage = "dx-download-agent download [-max_threads=N] <manifest.json.bz2>"

func (*downloadCmd) Name() string     { return "download" }
func (*downloadCmd) Synopsis() string { return "Download files in a manifest" }
func (*downloadCmd) Usage() string {
	return downloadUsage
}
func (p *downloadCmd) SetFlags(f *flag.FlagSet) {
	f.IntVar(&p.maxThreads, "max_threads", 8, "Maximum # of threads to use when downloading files")
}

func (p *downloadCmd) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	// TODO: Is there a generic way to do this using subcommands?
	if len(f.Args()) == 0 {
		fmt.Println(downloadUsage)
		os.Exit(1)
	}
	fname := f.Args()[0]
	token, method := dxda.GetToken()
	fmt.Printf("Obtained token using %s\n", method)
	var opts dxda.Opts
	opts.NumThreads = p.maxThreads
	if _, err := os.Stat(fname + ".stats.db"); os.IsNotExist(err) {
		fmt.Printf("Creating manifest database %s\n", fname+".stats.db")
		dxda.CreateManifestDB(fname)
	}
	if err := dxda.CheckDiskSpace(fname); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	dxda.DownloadManifestDB(fname, token, opts)
	return subcommands.ExitSuccess
}

type progressCmd struct {
	maxThreads int
}

func (*progressCmd) Name() string     { return "progress" }
func (*progressCmd) Synopsis() string { return "Download files in a manifest" }

const progressUsage = "dx-download-agent progress <manifest.json.bz2>"

func (*progressCmd) Usage() string {
	return progressUsage
}
func (p *progressCmd) SetFlags(f *flag.FlagSet) {
}
func (p *progressCmd) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	// TODO: Is there a generic way to do this using subcommands?
	if len(f.Args()) == 0 {
		fmt.Println(progressUsage)
		os.Exit(1)
	}
	fname := f.Args()[0]
	fmt.Println(dxda.DownloadProgress(fname))
	return subcommands.ExitSuccess
}

type inspectCmd struct {
	maxThreads int
}

const inspectUsage = "dx-download-agent inspect [-max_threads=N] <manifest.json.bz2>"

func (*inspectCmd) Name() string { return "inspect" }
func (*inspectCmd) Synopsis() string {
	return "Inspect files downloaded in a manifest + additional 'health' checks"
}
func (*inspectCmd) Usage() string {
	return downloadUsage
}
func (p *inspectCmd) SetFlags(f *flag.FlagSet) {
	f.IntVar(&p.maxThreads, "max_threads", 8, "Maximum # of threads to use when inspecting files")
}

func (p *inspectCmd) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	// TODO: Is there a generic way to do this using subcommands?
	if len(f.Args()) == 0 {
		fmt.Println(inspectUsage)
		os.Exit(1)
	}
	fname := f.Args()[0]

	var opts dxda.Opts
	opts.NumThreads = p.maxThreads

	dxda.CheckFileIntegrity(fname, opts)
	return subcommands.ExitSuccess
}

// The CLI is simply a wrapper around the dxda package
func main() {
	subcommands.Register(subcommands.HelpCommand(), "")
	subcommands.Register(subcommands.FlagsCommand(), "")
	subcommands.Register(subcommands.CommandsCommand(), "")
	subcommands.Register(&downloadCmd{}, "")
	subcommands.Register(&progressCmd{}, "")
	subcommands.Register(&inspectCmd{}, "")

	// TODO: modify this to use individual subcommand help
	if len(os.Args) == 1 {
		fmt.Printf("Usage:\n  For progress:\n  $ %s\n\n  For downloading:\n  $ %s\n", progressUsage, downloadUsage)
		os.Exit(1)
	}

	flag.Parse()
	ctx := context.Background()
	os.Exit(int(subcommands.Execute(ctx)))
}
