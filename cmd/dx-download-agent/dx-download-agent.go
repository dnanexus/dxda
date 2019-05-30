package main

import (
	"context"
	"flag"
	"fmt"
	"log"
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

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func (p *downloadCmd) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	// TODO: Is there a generic way to do this using subcommands?
	if len(f.Args()) == 0 {
		fmt.Println(downloadUsage)
		os.Exit(1)
	}
	fname := f.Args()[0]
	logfname := fname + ".download.log"
	logfile, err := os.OpenFile(logfname, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	check(err)
	defer logfile.Close()
	log.SetOutput(logfile)

	dxda.PrintLogAndOut("Logging detailed output to: " + logfname + "\n")

	dxEnv, method, err := dxda.GetDxEnvironment()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	dxda.PrintLogAndOut(fmt.Sprintf("Obtained token using %s\n", method))
	dxda.SetDxEnvironment(dxEnv)

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
	dxda.DownloadManifestDB(fname, opts)
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
	ds := dxda.InitDownloadStatus(fname)
	fmt.Println(dxda.DownloadProgressOneTime(&ds, 60*1000*1000*1000))
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
