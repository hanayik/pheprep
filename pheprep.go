package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"strings"

	//"strconv"
	"github.com/korovkin/limiter"
)

func reformatFileFast(inputFilePath string, outputFilePath string, nVariants uint64) {

	// chrom := 0
	// pos := 2
	// alt := 3
	// ref := 4
	// beta := 5
	// sebeta := 6
	// pval := 7
	nVarInput := getTotalLines(inputFilePath)

	if nVarInput != nVariants {
		panic("nVariants and nVarInput do not match!")
	} else {
		fmt.Println("file lengths match")
	}

	inFileBytes, err := ioutil.ReadFile(inputFilePath) // just pass the file name
	if err != nil {
		fmt.Print(err)
		panic(err)
	}
	inFileStr := string(inFileBytes)

	splitStr := strings.Split(inFileStr, "\n")

	// free some memory
	inFileStr = ""
	inFileBytes = nil
	// firstLine := 0
	for i := range splitStr {
		fields := strings.Fields(splitStr[i])
		if i == 0 {
			fmt.Println(fields)
		}
	}

	if err := writeLines(splitStr, outputFilePath); err != nil {
		log.Fatalf("writeLines: %s", err)
	}
}

func writeLines(lines []string, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	for _, line := range lines {
		fmt.Fprintln(w, line)
	}
	return w.Flush()
}

// func reformatFileSlow(inputFilePath string, outputFilePath string, nVariants uint64) {
// 	//reformat an input file to be ready for pheweb
// 	//chr rsid pos a1 a2 beta se pval(-log10)

// 	// check that input file number of variants equals the value from the variants file
// 	nVarInput := getTotalLines(inputFilePath)

// 	if nVarInput != nVariants {
// 		panic("nVariants and nVarInput do not match!")
// 	} else {
// 		fmt.Println("file lengths match")
// 	}

// 	chrom := 0
// 	pos := 2
// 	alt := 3
// 	ref := 4
// 	beta := 5
// 	sebeta := 6
// 	pval := 7

// 	// open input file
// 	inFile, err := os.Open(inputFilePath)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	fmt.Println("read in file ", inputFilePath)

// 	defer inFile.Close()

// 	// open output file for writing
// 	outFile, err := os.Create(outputFilePath)
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer outFile.Close()

// 	outputWriter := bufio.NewWriter(outFile)

// 	scanner := bufio.NewScanner(inFile)
// 	idx := 0
// 	firstLine := 0
// 	for scanner.Scan() {
// 		if idx == firstLine {
// 			fmt.Fprintf(outputWriter, "chrom\tpos\tref\talt\tbeta\tsebeta\tpval\n")
// 		} else {
// 			lineText := scanner.Text()

// 			fields := strings.Fields(lineText)

// 			// remove left padded zeros
// 			chromVal := fields[chrom]
// 			if strings.HasPrefix(chromVal, "0") {
// 				chromVal = strings.TrimPrefix(chromVal, "0")
// 			}

// 			fmt.Fprintf(outputWriter, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", chromVal, fields[pos], fields[ref], fields[alt], fields[beta], fields[sebeta], fields[pval])
// 			outputWriter.Flush()

// 		}

// 		idx += 1

// 	}

// }

func getTotalLines(filePath string) uint64 {
	// open input file
	inFile, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer inFile.Close()

	var lineCount uint64 = 0

	scanner := bufio.NewScanner(inFile)
	for scanner.Scan() {
		lineCount++
	}
	return lineCount
}

func main() {

	njobs := flag.Int("jobs", -1, "The number of concurrent jobs to run (-1 uses all cpu cores)")
	variantFile := flag.String("var", "", "The variant file that contains af and maf")

	//outFile := flag.String("o", "", "relative or full path of the output file name")

	ncpus := runtime.NumCPU()
	runtime.GOMAXPROCS(ncpus)
	fmt.Println("number of cpus ", ncpus)
	// var wg sync.WaitGroup
	flag.Parse()

	var nVariants uint64 = 0
	if *variantFile != "" {
		nVariants = getTotalLines(*variantFile)
	}

	fileArray := flag.Args()

	if *njobs == -1 {
		*njobs = ncpus
	} else if *njobs > ncpus {
		*njobs = ncpus
	}
	// fmt.Println("using njobs: ", *njobs)
	limit := limiter.NewConcurrencyLimiter(*njobs)

	// wg.Add(len(fileArray))
	for _, inFile := range fileArray {
		outFile := inFile + ".out"
		limit.Execute(func() {
			// reformatFile(inFile, outFile, nVariants)
			reformatFileFast(inFile, outFile, nVariants)
		})
	}

	limit.Wait()
}
