package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"github.com/korovkin/limiter"
)

func reformatFile(inputFilePath string, outputFilePath string) {
	//reformat an input file to be ready for pheweb

	// defer wg.Done()
	//chr rsid pos a1 a2 beta se pval(-log10)
	chrom := 0
	pos := 2
	alt := 3
	ref := 4
	beta := 5
	sebeta := 6
	pval := 7

	// open input file
	inFile, err := os.Open(inputFilePath)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("read in file ", inputFilePath)

	defer inFile.Close()

	// open output file for writing
	outFile, err := os.Create(outputFilePath)
	if err != nil {
		panic(err)
	}
	defer outFile.Close()

	outputWriter := bufio.NewWriter(outFile)

	scanner := bufio.NewScanner(inFile)
	idx := 0
	firstLine := 0
	for scanner.Scan() {
		if idx == firstLine {
			fmt.Fprintf(outputWriter, "chrom\tpos\tref\talt\tbeta\tsebeta\tpval\n")
		} else {
			lineText := scanner.Text()

			fields := strings.Fields(lineText)

			// remove left padded zeros
			chromVal := fields[chrom]
			if strings.HasPrefix(chromVal, "0") {
				chromVal = strings.TrimPrefix(chromVal, "0")
			}

			fmt.Fprintf(outputWriter, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", chromVal, fields[pos], fields[ref], fields[alt], fields[beta], fields[sebeta], fields[pval])
			outputWriter.Flush()

		}

		idx += 1

	}
}

func main() {

	njobs := flag.Int("j", -1, "The number of concurrent jobs to run (-1 uses all cpu cores)")

	//outFile := flag.String("o", "", "relative or full path of the output file name")

	ncpus := runtime.NumCPU()
	runtime.GOMAXPROCS(ncpus)
	fmt.Println("number of cpus ", ncpus)
	// var wg sync.WaitGroup
	flag.Parse()

	fileArray := flag.Args()

	if *njobs == -1 {
		*njobs = ncpus
	} else if *njobs > ncpus {
		*njobs = ncpus
	}
	fmt.Println("using njobs: ", *njobs)
	limit := limiter.NewConcurrencyLimiter(*njobs)

	// wg.Add(len(fileArray))
	for _, inFile := range fileArray {
		outFile := inFile + ".out"
		limit.Execute(func() {
			reformatFile(inFile, outFile)
		})
	}

	limit.Wait()

	// wg.Wait()
	//fmt.Println("input file is ", *inFile)
	//fmt.Println("output file is ", *outFile)
	//reformatFile(*inFile, *outFile)
}
