package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	// The dxda package should contain all core functionality
	"github.com/dnanexus/dxda"
	"github.com/google/subcommands"
)

// download subcommand
type downloadCmd struct {
	numThreads int
	verbose    bool
	gcInfo     bool
}

var err error

const rootDescription = "CLI tool to manage the download of files from DNAnexus"
const downloadUsage = "dx-download-agent download [-num_threads=N] <manifest.json.bz2>"
const flagsUsage = "dx-download-agent flags [<subcommand>]"
const inspectSynopsis = "Inspect files downloaded in a manifest and validate their integrity"
const versionUsage = "dx-download-agent version"

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
	} else {
		// Check database schema is compatible with current version
		if err := st.CheckSchemaVersion(); err != nil {
			fmt.Printf("Error: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Ensuring files are created for existing manifest \n")
		st.PrepareFilesForDownload(*manifest)
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
func (*progressCmd) Synopsis() string { return "Show current download progress" }

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
	fmt.Println(st.DownloadProgressOneTime(60 * 1000 * 1000 * 1000))
	return subcommands.ExitSuccess
}

// inspect the files, and see that there are no checksum errors
type inspectCmd struct {
	numThreads int
	verbose    bool
}

const inspectUsage = "dx-download-agent inspect [-num_threads=N] <manifest.json.bz2>"

func (*inspectCmd) Name() string { return "inspect" }
func (*inspectCmd) Synopsis() string {
	return inspectSynopsis
}
func (*inspectCmd) Usage() string {
	return inspectUsage
}
func (p *inspectCmd) SetFlags(f *flag.FlagSet) {
	f.IntVar(&p.numThreads, "num_threads", 0, "Number of threads to use when validating files. By default (or if zero), this number is chosen according to machine memory and CPU constraints.")
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
	opts.NumThreads = p.numThreads

	st := dxda.NewDxDa(dxEnv, fname, opts)
	defer st.Close()

	// Check database schema is compatible with current version
	if err := st.CheckSchemaVersion(); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	integrityFlag := st.CheckFileIntegrity()
	if !integrityFlag {
		return subcommands.ExitFailure
	}
	return subcommands.ExitSuccess
}

// get the version
type versionCmd struct {
}

func (*versionCmd) Name() string               { return "version" }
func (*versionCmd) Synopsis() string           { return "Get the version" }
func (*versionCmd) Usage() string              { return versionUsage }
func (p *versionCmd) SetFlags(f *flag.FlagSet) {}
func (p *versionCmd) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	fmt.Println(dxda.Version)
	return subcommands.ExitSuccess
}

type flagsCmd struct{}

func (*flagsCmd) Name() string           { return "flags" }
func (*flagsCmd) Synopsis() string       { return "Describe all known top-level flags" }
func (*flagsCmd) Usage() string          { return flagsUsage }
func (*flagsCmd) SetFlags(*flag.FlagSet) {}
func (*flagsCmd) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	if f.NArg() > 1 {
		fmt.Fprintln(os.Stderr, flagsUsage)
		return subcommands.ExitUsageError
	}

	if f.NArg() == 0 {
		printFlags(os.Stdout, flag.CommandLine)
		return subcommands.ExitSuccess
	}

	cmd := findRegisteredCommand(f.Arg(0))
	if cmd == nil {
		fmt.Fprintf(os.Stderr, "Subcommand %s not understood\n", f.Arg(0))
		return subcommands.ExitFailure
	}

	subflags := flag.NewFlagSet(cmd.Name(), flag.PanicOnError)
	cmd.SetFlags(subflags)
	printFlags(os.Stdout, subflags)
	return subcommands.ExitSuccess
}

func printCommandSummary(w io.Writer, cmd subcommands.Command) {
	fmt.Fprintf(w, "  %-15s %s\n", cmd.Name(), cmd.Synopsis())
}

func visitRegisteredCommands(fn func(subcommands.Command)) {
	subcommands.DefaultCommander.VisitCommands(func(_ *subcommands.CommandGroup, cmd subcommands.Command) {
		fn(cmd)
	})
}

func findRegisteredCommand(name string) subcommands.Command {
	var found subcommands.Command
	visitRegisteredCommands(func(cmd subcommands.Command) {
		if found == nil && cmd.Name() == name {
			found = cmd
		}
	})
	return found
}

func printFlags(w io.Writer, fs *flag.FlagSet) {
	fs.VisitAll(func(f *flag.Flag) {
		name, usage := flag.UnquoteUsage(f)
		line := fmt.Sprintf("  -%s", f.Name)
		if name != "" {
			line += " " + name
		}
		fmt.Fprintln(w, line)

		trimmedUsage := strings.TrimSpace(usage)
		if trimmedUsage == "" {
			return
		}

		for _, usageLine := range strings.Split(trimmedUsage, "\n") {
			fmt.Fprintf(w, "      %s\n", strings.TrimSpace(usageLine))
		}
	})
}

func explainTopLevel(w io.Writer) {
	fmt.Fprintf(w, "dx-download-agent %s\n", dxda.Version)
	fmt.Fprintf(w, "%s\n\n", rootDescription)
	fmt.Fprintln(w, "Usage:")
	fmt.Fprintf(w, "  %s\n", downloadUsage)
	fmt.Fprintf(w, "  %s\n", inspectUsage)
	fmt.Fprintf(w, "  %s\n", progressUsage)
	fmt.Fprintf(w, "  %s\n\n", versionUsage)
	fmt.Fprintln(w, "Authentication:")
	fmt.Fprintln(w, "  Set DX_API_TOKEN or configure ~/.dnanexus_config/environment.json.")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Primary commands:")
	printCommandSummary(w, &downloadCmd{})
	printCommandSummary(w, &inspectCmd{})
	printCommandSummary(w, &progressCmd{})
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Reference commands:")
	printCommandSummary(w, &versionCmd{})
	printCommandSummary(w, subcommands.HelpCommand())
	printCommandSummary(w, &flagsCmd{})
	printCommandSummary(w, subcommands.CommandsCommand())
	fmt.Fprintln(w)
}

func explainCommand(w io.Writer, cmd subcommands.Command) {
	usage := strings.TrimSpace(cmd.Usage())
	if usage != "" {
		fmt.Fprintln(w, usage)
	}

	synopsis := strings.TrimSpace(cmd.Synopsis())
	if synopsis != "" {
		fmt.Fprintf(w, "\n%s\n", synopsis)
	}

	subflags := flag.NewFlagSet(cmd.Name(), flag.PanicOnError)
	cmd.SetFlags(subflags)

	var hasFlags bool
	subflags.VisitAll(func(*flag.Flag) {
		hasFlags = true
	})
	if !hasFlags {
		return
	}

	fmt.Fprintln(w)
	printFlags(w, subflags)
}

// The CLI is simply a wrapper around the dxda package
func main() {
	subcommands.DefaultCommander.Explain = explainTopLevel
	subcommands.DefaultCommander.ExplainCommand = explainCommand

	subcommands.Register(subcommands.HelpCommand(), "")
	subcommands.Register(&flagsCmd{}, "")
	subcommands.Register(subcommands.CommandsCommand(), "")
	subcommands.Register(&downloadCmd{}, "")
	subcommands.Register(&progressCmd{}, "")
	subcommands.Register(&inspectCmd{}, "")
	subcommands.Register(&versionCmd{}, "")

	// TODO: modify this to use individual subcommand help
	if len(os.Args) == 1 {
		explainTopLevel(os.Stderr)
		os.Exit(1)
	}

	flag.Parse()
	ctx := context.Background()
	os.Exit(int(subcommands.Execute(ctx)))
}
