package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

func main() {

	file, err := ioutil.TempFile(os.TempDir(), "pew")
	if err != nil {
		log.Fatal(err)
	}

	// if err = file.Close(); err != nil {
	// 	log.Fatal(err)
	// }

	// err = os.Remove(file.Name())

	// if err != nil {
	// 	log.Fatal(err)
	// }

	err = file.Truncate(0)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(file.Name())
}
