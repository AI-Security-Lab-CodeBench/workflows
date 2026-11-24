package main

import (
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"codebench/bashguard/internal/httpapi"
)

const address = ":8080"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	healthcheck := flag.Bool("healthcheck", false, "probe the local HTTP service")
	version := flag.Bool("version", false, "print the executable runtime version")
	flag.Parse()
	if *healthcheck {
		return probe()
	}
	executable, err := os.Open("/proc/self/exe")
	if err != nil {
		return fmt.Errorf("open running executable: %w", err)
	}
	digest := sha256.New()
	_, hashErr := io.Copy(digest, executable)
	if err := errors.Join(hashErr, executable.Close()); err != nil {
		return fmt.Errorf("hash running executable: %w", err)
	}
	pin := httpapi.RuntimeVersion(fmt.Sprintf("sha256:%x", digest.Sum(nil)))
	if *version {
		if _, err := fmt.Println(pin); err != nil {
			return fmt.Errorf("print runtime version: %w", err)
		}
		return nil
	}
	server := &http.Server{
		Addr: address, Handler: httpapi.New(pin), MaxHeaderBytes: 16 << 10,
		ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 45 * time.Second, IdleTimeout: 30 * time.Second,
	}
	return fmt.Errorf("serve Bashguard HTTP: %w", server.ListenAndServe())
}

func probe() error {
	client := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{}}
	response, err := client.Get("http://127.0.0.1" + address + "/healthz")
	if err != nil {
		return fmt.Errorf("probe Bashguard HTTP: %w", err)
	}
	if err := response.Body.Close(); err != nil {
		return fmt.Errorf("close health response: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("health check returned %s", response.Status)
	}
	return nil
}
