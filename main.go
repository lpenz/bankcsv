// Copyright (c) 2018 Leandro Lisboa Penz <lpenz@lpenz.org>
// This file is subject to the terms and conditions defined in
// file LICENSE, which is part of this source code package.

package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// Base types:

type transaction struct {
	ID          string
	Date        time.Time
	Description string
	Value       string
	Account     string
	SrcAccount  string
}

// json config parsing: ///////////////////////////////////////////////////////

type config struct {
	AccountFromDescription []accountFromDescription
}

type accountFromDescription struct {
	Account string
	Regex   string
}

func configFromJSON(jsonName *string) (config, error) {
	dat, err := ioutil.ReadFile(*jsonName)
	var cfg config
	if err != nil {
		return cfg, err
	}
	if err := json.Unmarshal(dat, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// output formats: ////////////////////////////////////////////////////////////

// CSV:

type outputCsvFormat struct {
	outFd  *os.File
	outCsv *csv.Writer
}

func (o *outputCsvFormat) Init(outFd *os.File) {
	o.outFd = outFd
	_, err := outFd.WriteString("\"id\",\"date\",\"description\",\"withdrawal\",\"account\"\n")
	if err != nil {
		log.Fatalln("error writing csv header:", err)
	}
	o.outCsv = csv.NewWriter(outFd)
}

func (o *outputCsvFormat) Add(t *transaction) {
	date := t.Date.Format("2006-01-02")
	src := []string{t.ID, date, t.Description, t.Value, t.SrcAccount}
	if err := o.outCsv.Write(src); err != nil {
		log.Fatalln("error writing src record to csv:", err)
	}
	if t.Account != "" {
		value := ""
		if t.Value[0] == '-' {
			value = t.Value[1:]
		} else {
			value = fmt.Sprintf("-%s", t.Value)
		}
		dst := []string{"", "", "", value, t.Account}
		if err := o.outCsv.Write(dst); err != nil {
			log.Fatalln("error writing dst record to csv:", err)
		}
	}

}

func (o *outputCsvFormat) Finish() {
	o.outCsv.Flush()
	if err := o.outCsv.Error(); err != nil {
		log.Fatal(err)
	}
}

// parser: ////////////////////////////////////////////////////////////////////

func ymdParse(line string, lastdate *time.Time, counter *int) (time.Time, int, time.Month, int) {
	date, err := time.Parse("02/01/2006", line)
	if err != nil {
		log.Fatal(err)
	}
	if date != *lastdate {
		*counter = 1
		*lastdate = date
	}
	y, m, d := date.Date()
	return date, y, m, d
}

func valueParse(line []string, iscredit bool) (value string) {
	if iscredit {
		value = strings.TrimSpace(line[3])
	} else {
		value = strings.TrimSpace(line[5])
	}
	if value == "0.00" || value == "" {
		if iscredit {
			value = "-" + strings.TrimSpace(line[4])
		} else {
			value = "-" + strings.TrimSpace(line[6])
		}
	}
	return value
}

func lineParse(line []string, iscredit bool, lastdate *time.Time, counter *int) transaction {
	date, year, month, day := ymdParse(line[1], lastdate, counter)
	value := valueParse(line, iscredit)
	return transaction{
		ID:          fmt.Sprintf("%04d%02d%02d%02d", year, month, day, *counter),
		Date:        date,
		Description: line[2],
		Value:       value,
	}
}

func inputsParse(inputNames []string) <-chan *transaction {
	out := make(chan *transaction)
	go func() {
		defer close(out)
		lastdate := time.Now()
		counter := 1
		for _, inputName := range inputNames {
			inputFd, err := os.Open(filepath.Clean(inputName))
			if err != nil {
				log.Fatal(err)
			}
			inputBuf := bufio.NewReader(inputFd)
			inputCsv := csv.NewReader(inputBuf)
			var iscredit bool
			for {
				line, err := inputCsv.Read()
				if err == io.EOF {
					break
				} else if err != nil {
					log.Fatal(err)
				}
				if line[1] == " Posted Transactions Date" {
					switch line[0] {
					case "Masked Card Number":
						iscredit = true
					case "Posted Account":
						iscredit = false // "debit"
					default:
						log.Panicf("unknown input format for %s", inputName)
					}
					continue
				}
				t := lineParse(line, iscredit, &lastdate, &counter)
				out <- &t
				counter++
			}
		}
	}()
	return out
}

// processor //////////////////////////////////////////////////////////////////

func processCsvs(srcAccount *string, jsonName *string, outputName *string, inputNames []string) {
	cfg, err := configFromJSON(jsonName)
	if err != nil {
		panic(err)
	}
	var outFd *os.File
	if *outputName == "-" {
		outFd = os.Stdout
	} else {
		var err error
		outFd, err = os.Create(*outputName)
		if err != nil {
			log.Fatal("Error creating file", err)
		}
		defer func() {
			err := outFd.Close()
			if err != nil {
				log.Panicf("error closing %s: %s", *outputName, err)
			}
		}()
	}
	o := outputCsvFormat{}
	o.Init(outFd)
	for t := range inputsParse(inputNames) {
		t.SrcAccount = *srcAccount
		found := false
		for _, descAcc := range cfg.AccountFromDescription {
			match, err := regexp.MatchString(descAcc.Regex, t.Description)
			if err != nil {
				log.Panicf("error in MatchString: %s", err)
			}
			if match {
				t.Account = descAcc.Account
				found = true
			}
		}
		if found {
			o.Add(t)
		} else {
			log.Printf("could not assign account to %s", t.Description)
		}
	}
	o.Finish()
}

func main() {
	outputName := flag.String("o", "-", "output file")
	flag.Parse()
	if flag.NArg() != 3 {
		fmt.Fprintf(os.Stderr, "Wrong number of arguments\n")                                  // nolint: errcheck
		fmt.Fprintf(os.Stderr, "Usage: bankcsv <srcAccount> <json config file> <inputs...>\n") // nolint: errcheck
		flag.PrintDefaults()
		os.Exit(1)
	}
	args := flag.Args()
	srcAccount := &args[0]
	jsonName := &args[1]
	inputNames := args[2:]
	processCsvs(srcAccount, jsonName, outputName, inputNames)
}
