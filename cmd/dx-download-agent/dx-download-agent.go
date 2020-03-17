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

// download subcommand
type downloadCmd struct {
	numThreads int
	verbose bool
	gcInfo bool
}

const downloadUsage = "dx-download-agent download [-max_threads=N] <manifest.json.bz2>"

func (*downloadCmd) Name() string     { return "download" }
func (*downloadCmd) Synopsis() string { return "Download files in a manifest" }
func (*downloadCmd) Usage() string {
	return downloadUsage
}
func (p *downloadCmd) SetFlags(f *flag.FlagSet) {
	f.IntVar(&p.numThreads, "num_threads", 0, "Number of threads to use when downloading files. By default (or if zero), this number is chosen according to machine memory and CPU constraints.")
	f.IntVar(&p.numThreads, "max_threads", 0, "An alias for num_threads")
	f.BoolVar(&p.verbose, "verbose", false, "verbose logging")
	f.BoolVar(&p.gcInfo, "gc_info", false, "report statistics for golang garbage collection")
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
	dxda.PrintLogAndOut("Obtained token using %s\n", method)

	var opts dxda.Opts
	opts.NumThreads = p.numThreads
	opts.Verbose = p.verbose
	opts.GcInfo = p.gcInfo

	st := dxda.NewDxDa(dxEnv, fname, opts)
	defer st.Close()

	// read the manifest from disk, and fill in missing
	// details.
	manifest, err := dxda.ReadManifest(fname, &dxEnv)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// setup a persistent database to track all downloads
	if _, err := os.Stat(fname + ".stats.db"); os.IsNotExist(err) {
		fmt.Printf("Creating manifest database %s\n", fname+".stats.db")
		st.CreateManifestDB(*manifest, fname)
	}

	if err := st.CheckDiskSpace(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// start a parallel download
	st.DownloadManifestDB(fname)

	return subcommands.ExitSuccess
}

type progressCmd struct {
}
func (*progressCmd) Name() string     { return "progress" }
func (*progressCmd) Synopsis() string { return "show current download progress" }

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

	dxEnv, _, err := dxda.GetDxEnvironment()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	var opts dxda.Opts
	st := dxda.NewDxDa(dxEnv, fname, opts)
	defer st.Close()

	st.InitDownloadStatus()
	fmt.Println(st.DownloadProgressOneTime(60*1000*1000*1000))
	return subcommands.ExitSuccess
}

// inspect the files, and see that there are no checksum errors
type inspectCmd struct {
	verbose bool
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
	f.BoolVar(&p.verbose, "verbose", false, "verbose logging")
}

func (p *inspectCmd) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	// TODO: Is there a generic way to do this using subcommands?
	if len(f.Args()) == 0 {
		fmt.Println(inspectUsage)
		os.Exit(1)
	}
	fname := f.Args()[0]

	dxEnv, _, err := dxda.GetDxEnvironment()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	var opts dxda.Opts
	opts.Verbose = p.verbose

	st := dxda.NewDxDa(dxEnv, fname, opts)
	defer st.Close()

	integrityFlag := st.CheckFileIntegrity()
	if !integrityFlag {
		return subcommands.ExitFailure
	}
	return subcommands.ExitSuccess
}

// get the version
type versionCmd struct {
}

func (*versionCmd) Name() string     { return "version" }
func (*versionCmd) Synopsis() string { return "get the version" }
func (*versionCmd) Usage() string    { return   "get the dx-download-agent version" }
func (p *versionCmd) SetFlags(f *flag.FlagSet) {}
func (p *versionCmd) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	fmt.Println(dxda.Version)
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
	subcommands.Register(&versionCmd{}, "")

	// TODO: modify this to use individual subcommand help
	if len(os.Args) == 1 {
		fmt.Printf("Usage:\n  For progress:\n  $ %s\n\n  For downloading:\n  $ %s\n", progressUsage, downloadUsage)
		os.Exit(1)
	}

	flag.Parse()
	ctx := context.Background()
	os.Exit(int(subcommands.Execute(ctx)))
}
