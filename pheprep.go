package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"flag"
	"sync"
	"runtime"
)


func reformatFile(wg *sync.WaitGroup, inputFilePath string, outputFilePath string) {
	//reformat an input file to be ready for pheweb

	defer wg.Done()
	//chr rsid pos a1 a2 beta se pval(-log10)
	chrom 	:= 0
	pos	:= 2
	alt	:= 3
	ref	:= 4
	beta	:= 5
	sebeta	:= 6
	pval	:= 7

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
		if idx==firstLine {
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

	//inFile := flag.String("i", "", "The input file from biobank IDP processing")

	//outFile := flag.String("o", "", "relative or full path of the output file name")

	ncpus := runtime.NumCPU()
	fmt.Println(ncpus)
	var wg sync.WaitGroup

	flag.Parse()

	fileArray := flag.Args()
	for _, inFile := range fileArray {
		outFile := inFile + ".out"
		wg.Add(1)
		go reformatFile(&wg, inFile, outFile)
	}

	wg.Wait()
	//fmt.Println("input file is ", *inFile)
	//fmt.Println("output file is ", *outFile)
	//reformatFile(*inFile, *outFile)
}

