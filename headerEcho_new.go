package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

func helloWorld(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		reqBody, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("%s\n", reqBody)
		w.Write([]byte(reqBody))
	default:
		for k, v := range r.Header {
			fmt.Fprintf(w, "%q: %q\n", k, v)
		}
	}
	fmt.Fprintf(w, "%q", r.Proto)

}

func main() {
	http.HandleFunc("/", helloWorld)
	http.ListenAndServe(":4000", nil)
}
