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

func inputsParse(inputNames []string) <-chan *transaction {
	out := make(chan *transaction)
	go func() {
		defer close(out)
		lastdate := time.Now()
		counter := 1
		for _, inputName := range inputNames {
			inputFd, err := os.Open(inputName)
			if err != nil {
				log.Fatal(err)
			}
			inputBuf := bufio.NewReader(inputFd)
			inputCsv := csv.NewReader(inputBuf)
			inputFmt := ""
			for {
				line, err := inputCsv.Read()
				if err == io.EOF {
					break
				} else if err != nil {
					log.Fatal(err)
				}
				if line[1] == " Posted Transactions Date" {
					if line[0] == "Masked Card Number" {
						inputFmt = "credit"
					} else if line[0] == "Posted Account" {
						inputFmt = "debit"
					} else {
						log.Panicf("unknown input format for %s", inputName)
					}
					continue
				}
				var date time.Time
				date, err = time.Parse("02/01/2006", line[1])
				if err != nil {
					log.Fatal(err)
				}
				if date != lastdate {
					counter = 1
					lastdate = date
				}
				year, month, day := date.Date()
				var value string
				if inputFmt == "credit" {
					value = strings.TrimSpace(line[3])
				} else {
					value = strings.TrimSpace(line[5])
				}
				if value == "0.00" || value == "" {
					if inputFmt == "credit" {
						value = "-" + strings.TrimSpace(line[4])
					} else {
						value = "-" + strings.TrimSpace(line[6])
					}
				}
				t := transaction{
					ID:          fmt.Sprintf("%04d%02d%02d%02d", year, month, day, counter),
					Date:        date,
					Description: line[2],
					Value:       value,
				}
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
		defer outFd.Close()
	}
	o := outputCsvFormat{}
	o.Init(outFd)
	for t := range inputsParse(inputNames) {
		t.SrcAccount = *srcAccount
		found := false
		for _, descAcc := range cfg.AccountFromDescription {
			match, _ := regexp.MatchString(descAcc.Regex, t.Description)
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
