// Copyright 2017 Kochava
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"log"
	"os"

	"github.com/urfave/cli"
)

//NOTE :: Still using globals - blegh

//Conf is the Global config object
var Conf Config //Access as global for easy flag config

func init() {
	//Ensure config object has been created with empty value
	Conf = Config{}
}

func initLogging(c *cli.Context, conf *Config) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if conf.STDOutLogging {
		return // Essentially a noop to skip setting up the log file
	}

	logFile, err := os.Create(conf.LogFile)
	if err != nil {
		log.Printf("Unable to open log file: %s\n", err)
		log.Println("Reverting to stdout logging")
		return // log already goes to stdout so just early exit without calling log.SetOutput
	}

	log.SetOutput(logFile)
}

func main() {
	app := cli.NewApp()
	app.Name = "Kochava Kafka Transfer Agent"
	app.Usage = "An agent which consumes a topic from one set of brokers and publishes to another set of brokers"
	app.Flags = AppConfigFlags //defined in flags.go
	//Major, minor, patch version
	app.Version = "0.3.0"
	app.Action = func(c *cli.Context) error {
		initLogging(c, &Conf)

		Conf.SourceZookeepers = c.StringSlice("src-zookeepers")
		Conf.DestinationZookeepers = c.StringSlice("dst-zookeepers")

		log.Printf("Config %v", Conf)

		if err := startFirehose(c, &Conf); err != nil {
			log.Fatal(err)
		}
		return nil
	}
	app.Run(os.Args)
}
