package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func formatDefaultDate() string {
	year, month, day := time.Now().Date()

	return fmt.Sprintf("%4d%02d%02d", year, int(month), day)
}

func formatDate(date string) string {
	return strings.Replace(strings.Replace(date, "/", "", -1), "-", "", -1)
}

const (
	version     = "0.1"
	MAXATTEMPTS = 3
)

var (
	regionName      string
	bucketName      string
	dateFrom        string
	dateTo          string
	msoListFilename string
	maxAttempts     int
	concurrency     int

	verbose bool
	appName string
)

func init() {

	flagRegion := flag.String("r", "us-west-2", "`AWS Region`")
	flagBucket := flag.String("b", "daap-hh-count", "`Bucket name`")
	flagDateFrom := flag.String("from", formatDefaultDate(), "`Date from`")
	flagDateTo := flag.String("to", formatDefaultDate(), "`Date to`")
	flagMsoFileName := flag.String("m", "mso-list.csv", "Filename for `MSO` list")
	flagMaxAttempts := flag.Int("M", MAXATTEMPTS, "`Max attempts` to retry download from aws.s3")
	flagConcurrency := flag.Int("c", 10, "The number of files to process `concurrent`ly")

	flagVerbose := flag.Bool("v", true, "`Verbose`: outputs to the screen")

	flag.Parse()
	if flag.Parsed() {
		regionName = *flagRegion
		bucketName = *flagBucket
		dateFrom = formatDate(*flagDateFrom)
		dateTo = formatDate(*flagDateTo)
		msoListFilename = *flagMsoFileName
		maxAttempts = *flagMaxAttempts
		concurrency = *flagConcurrency

		verbose = *flagVerbose
		appName = os.Args[0]
	} else {
		usage()
	}

}

func usage() {
	fmt.Printf("%s, ver. %s\n", appName, version)
	fmt.Println("Command line:")
	fmt.Printf("\tprompt$>%s -r <aws_region> -b <s3_bucket_name> --from <date> --to <date> -m <mso-list-file-name> -M <max_retry>\n", appName)
	flag.Usage()
	os.Exit(-1)
}

func PrintParams() {
	log.Printf("Provided: -r: %s, -b: %s, --from: %v, --to: %v, -m %s, -M %d, -v: %v\n",
		regionName,
		bucketName,
		dateFrom,
		dateTo,
		msoListFilename,
		maxAttempts,
		verbose,
	)

}

type MsoType struct {
	Code string
	Name string
}

func getMsoNamesList() []MsoType {
	msoList := []MsoType{}

	msoFile, err := os.Open(msoListFilename)
	if err != nil {
		log.Fatalf("Could not open Mso List file: %s, Error: %s\n", msoListFilename, err)
	}

	r := csv.NewReader(msoFile)
	records, err := r.ReadAll()
	if err != nil {
		log.Fatalf("Could not read MSO file: %s, Error: %s\n", msoListFilename, err)
	}

	for _, record := range records {
		msoList = append(msoList, MsoType{record[0], record[1]})
	}
	return msoList
}

func formatPrefix(path, msoCode string) string {
	return fmt.Sprintf("%s/%s/delta/", path, msoCode)
}

var (
	failedFilesChan         chan string
	downloadedReportChannel chan bool
	dwnldMu                 sync.Mutex
)

func convertToDateParts(dtStr string) (yy, mm, dd int) {
	yy, mm, dd = 0, 0, 0
	i, err := strconv.Atoi(dtStr[:4])
	if err != nil {
		return yy, mm, dd
	}
	yy = i

	i, err = strconv.Atoi(dtStr[4:6])
	if err != nil {
		return yy, mm, dd
	}
	mm = i

	i, err = strconv.Atoi(dtStr[7:])
	if err != nil {
		return yy, mm, dd
	}
	dd = i
	return yy, mm, dd
}

func getDateRangeRegEx(dateFrom, dateTo string) []string {

	regExpStr := []string{}
	//'20160630'
	yy, mm, dd := convertToDateParts(dateFrom)
	dtFrom := time.Date(yy, time.Month(mm), dd, 0, 0, 0, 0, time.UTC)
	if verbose {
		log.Println("From:", dtFrom.String())
	}

	yy, mm, dd = convertToDateParts(dateTo)
	dtTo := time.Date(yy, time.Month(mm), dd, 0, 0, 0, 0, time.UTC)
	if verbose {
		log.Println("To:", dtTo.String())
	}

	dt := dtFrom
	for {
		regExpStr = append(regExpStr, dt.Format("20060102"))
		if verbose {
			log.Printf("Appending for %s = %s\n", dt.String(), dt.Format("20060102"))
		}

		dt = dt.AddDate(0, 0, 1)
		if dt.After(dtTo) {
			break
		}
	}

	return regExpStr
}

func printRangeString(dateRangeRegexStr []string) {
	log.Println("Dates range:")
	for _, str := range dateRangeRegexStr {
		log.Println(str)
	}
}
func main() {
	startTime := time.Now()
	downloaded := 0

	// This is our semaphore/pool
	sem := make(chan bool, concurrency)
	downloadedReportChannel = make(chan bool)

	msoList := getMsoNamesList()

	if verbose {
		PrintParams()
	}

	dateRangeRegexStr := getDateRangeRegEx(dateFrom, dateTo)

	failedFilesList := []string{}
	var wg sync.WaitGroup

	// Listening to failed reports
	go func() {
		for {
			key, more := <-failedFilesChan
			if more {
				failedFilesList = append(failedFilesList, key)
			} else {
				return
			}
		}
	}()

	// listening to succeeded reports
	go func() {
		for {
			_, more := <-downloadedReportChannel
			if more {
				dwnldMu.Lock()
				downloaded++
				dwnldMu.Unlock()
			} else {
				return
			}
		}
	}()

	session := session.New(&aws.Config{
		Region: aws.String(regionName),
	})

	svc := s3.New(session)

	params := &s3.ListObjectsInput{
		Bucket: aws.String(bucketName), // daap-hh-count
		Prefix: aws.String("cdw-data-reports"),
	}

	// Get the list of all objects
	resp, err := svc.ListObjects(params)
	if err != nil {
		log.Println("Failed to list objects: ", err)
		os.Exit(-1)
	}

	log.Println("Number of objects: ", len(resp.Contents))
	for _, key := range resp.Contents {
		// iterate through the list to match the dates range/mso name
		// using the constracted below regex

		log.Println("Key: ", *key.Key)

		for _, mso := range msoList {

			for _, eachDate := range dateRangeRegexStr {
				// cdw-data-reports/20160601/ Armstrong-Butler/hhid_count- Armstrong-Butler-20160601.csv
				lookupKey := fmt.Sprintf("%s-%s.csv", mso.Name, eachDate)
				log.Println("Lookup key: ", lookupKey)

				if strings.Contains(*key.Key, lookupKey) {
					// download the file (add to a queue of downloads)
					// load the csv file, add the count to appropriate counter
					// if we still have available goroutine in the pool (out of concurrency )
					sem <- true
					wg.Add(1)
					go func(key string) {
						defer func() { <-sem }()
						processSingleDownload(key, &wg)
					}(*key.Key)
				}
			}

		}
	}
	// aggregate the counts and generate the aggregated report
	// save the report csv?

	// Reports
	if verbose {
		log.Println("All files sent to be downloaded. Waiting for completetion...")
	}

	for i := 0; i < cap(sem); i++ {
		sem <- true
	}

	wg.Wait()
	if verbose {
		log.Println("All download jobs completed, closing failed/succeeded jobs channel")
	}
	close(failedFilesChan)
	close(downloadedReportChannel)
	ReportFailedFiles(failedFilesList)
	dwnldMu.Lock()
	downloadedVal := downloaded
	dwnldMu.Unlock()
	log.Printf("Processed %d MSO's, %d files, in %v\n", len(msoList), downloadedVal, time.Since(startTime))
}

func ReportFailedFiles(failedFilesList []string) {
	if len(failedFilesList) > 0 {
		for _, key := range failedFilesList {
			log.Println("Failed downloading: ", key)
		}
	} else {
		log.Println("No failed downloads")
	}
}

func processSingleDownload(key string, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < maxAttempts; i++ {
		log.Println("Downloading: ", key)
		if downloadFile(key) {
			if verbose {
				log.Println("Successfully downloaded: ", key)
			}
			downloadedReportChannel <- true
			return
		} else {
			if verbose {
				log.Println("Failed, going to sleep for: ", key)
			}
			time.Sleep(time.Duration(10) * time.Second)
		}
	}
	failedFilesChan <- key
}

func createPath(path string) error {
	err := os.MkdirAll(filepath.Dir(path), os.ModePerm)
	return err
}

func downloadFile(filename string) bool {

	err := createPath(filename)
	if err != nil {
		log.Println("Could not create folder: ", filepath.Dir(filename))
		return false
	}

	file, err := os.Create(filename)
	if err != nil {
		log.Println("Failed to create file: ", err)
		return false
	}

	defer file.Close()

	downloader := s3manager.NewDownloader(session.New(&aws.Config{Region: aws.String(regionName)}))

	numBytes, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(filename),
		})

	if err != nil {
		log.Printf("Failed to download file: %s, Error: %s ", filename, err)
		return false
	}

	log.Println("Downloaded file ", file.Name(), numBytes, " bytes")
	return true
}
