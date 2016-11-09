/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"io"
	"time"

	"github.com/minio/minio/pkg/lock"
)

// Returns if the prefix is a multipart upload.
func (fs fsObjects) isMultipartUpload(bucket, prefix string) bool {
	uploadsIDPath := pathJoin(fs.fsPath, bucket, prefix, uploadsJSONFile)
	_, err := fsStatFile(uploadsIDPath)
	if err != nil {
		if err == errFileNotFound {
			return false
		}
		errorIf(err, "Unable to access uploads.json "+uploadsIDPath)
		return false
	}
	return true
}

// isUploadIDExists - verify if a given uploadID exists and is valid.
func (fs fsObjects) isUploadIDExists(bucket, object, uploadID string) bool {
	uploadIDPath := pathJoin(bucket, object, uploadID)
	_, err := fsStatFile(pathJoin(fs.fsPath, minioMetaMultipartBucket, uploadIDPath, fsMetaJSONFile))
	if err != nil {
		if err == errFileNotFound {
			return false
		}
		errorIf(err, "Unable to access upload id "+uploadIDPath)
		return false
	}
	return true
}

// Removes the uploadID, called either by CompleteMultipart of AbortMultipart. If the resuling uploads
// slice is empty then we remove/purge the file.
func (fs fsObjects) removeUploadID(bucket, object, uploadID string, rwlk *lock.RWLockedFile) error {
	uploadIDs := uploadsV1{}
	_, err := uploadIDs.ReadFrom(io.NewSectionReader(rwlk, 0, rwlk.Size()))
	if err != nil {
		return err
	}

	// Removes upload id from the uploads list.
	uploadIDs.RemoveUploadID(uploadID)

	// Check this is the last entry.
	if uploadIDs.IsEmpty() {
		// No more uploads left, so we delete `uploads.json` file.
		uploadsPath := pathJoin(bucket, object, uploadsJSONFile)
		multipartPathBucket := pathJoin(fs.fsPath, minioMetaMultipartBucket)
		uploadPathBucket := pathJoin(multipartPathBucket, uploadsPath)
		return fsDeleteFile(multipartPathBucket, uploadPathBucket)
	} // else not empty

	// Write update `uploads.json`.
	_, err = uploadIDs.WriteTo(rwlk)
	return err
}

// Adds a new uploadID if no previous `uploads.json` is
// found we initialize a new one.
func (fs fsObjects) addUploadID(bucket, object, uploadID string, initiated time.Time, rwlk *lock.RWLockedFile) error {
	uploadIDs := uploadsV1{}

	_, err := uploadIDs.ReadFrom(io.NewSectionReader(rwlk, 0, rwlk.Size()))
	// For all unexpected errors, we return.
	if err != nil && errorCause(err) != io.EOF {
		return err
	}

	// If we couldn't read anything, we assume a default
	// (empty) upload info.
	if errorCause(err) == io.EOF {
		uploadIDs = newUploadsV1("fs")
	}

	// Adds new upload id to the list.
	uploadIDs.AddUploadID(uploadID, initiated)

	// Write update `uploads.json`.
	_, err = uploadIDs.WriteTo(rwlk)
	return err
}
