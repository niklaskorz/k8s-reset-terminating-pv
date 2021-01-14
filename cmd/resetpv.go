/*
Copyright (c) 2020 Jian Zhang
Licensed under MIT https://github.com/jianz/jianz.github.io/blob/master/LICENSE
*/

package cmd

import (
	"bytes"
	"context"
	"database/sql"
	"log"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3" // sqlite driver
	"github.com/spf13/cobra"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer/protobuf"
)

var (
	dbPath   string
	selector string

	cmd = &cobra.Command{
		Use:   "resetpv [flags] <persistent volume name>",
		Short: "Reset the Terminating PersistentVolume back to Bound status.",
		Long:  "Reset the Terminating PersistentVolume back to Bound status.\nPlease visit https://github.com/jianz/k8s-reset-terminating-pv for the detailed explanation.",
		Args: func(cmd *cobra.Command, args []string) error {
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			err := resetPV()
			return err
		},
	}
)

// Execute reset the Terminating PersistentVolume to Bound status.
func Execute() {
	cmd.Flags().StringVar(&dbPath, "db", "state.db", "Sqlite database")
	cmd.Flags().StringVar(&selector, "selector", "/registry/persistentvolumes/%", "The key selector for kubernetes resources.")

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func resetPV() error {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return recoverPV(ctx, db)
}

func recoverPV(ctx context.Context, db *sql.DB) error {

	gvk := schema.GroupVersionKind{Group: v1.GroupName, Version: "v1", Kind: "PersistentVolume"}
	pv := &v1.PersistentVolume{}

	runtimeScheme := runtime.NewScheme()
	runtimeScheme.AddKnownTypeWithName(gvk, pv)
	protoSerializer := protobuf.NewSerializer(runtimeScheme, runtimeScheme)

	// Get PV value from sqlite
	rows, err := db.Query("SELECT name, value FROM kine WHERE name LIKE ?", selector)
	if err != nil {
		return err
	}
	for rows.Next() {
		name := ""
		data := new([]byte)
		if err := rows.Scan(&name, data); err != nil {
			return err
		}

		// Decode protobuf value to PV struct
		_, _, err := protoSerializer.Decode(*data, &gvk, pv)
		if err != nil {
			return err
		}

		// Set PV status from Terminating to Bound by removing value of DeletionTimestamp and DeletionGracePeriodSeconds
		if (*pv).ObjectMeta.DeletionTimestamp == nil {
			log.Printf("Skipped: persistent volume [%s] is not in terminating status\n", name)
			continue
		}
		log.Printf("Resetting persistent volume [%s]\n", name)
		(*pv).ObjectMeta.DeletionTimestamp = nil
		(*pv).ObjectMeta.DeletionGracePeriodSeconds = nil

		// Encode fixed PV struct to protobuf value
		var fixedPV bytes.Buffer
		err = protoSerializer.Encode(pv, &fixedPV)
		if err != nil {
			return err
		}

		// Write the updated protobuf vale back to sqlite
		if _, err := db.Exec("UPDATE kine SET value = ? WHERE name = ?", fixedPV.Bytes(), name); err != nil {
			return err
		}
	}

	return nil
}
