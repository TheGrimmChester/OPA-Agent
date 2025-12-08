package main

import (
	"github.com/pierrec/lz4/v4"
)

const (
	COMPRESSION_HEADER = "LZ4"
)

// decompressLZ4 decompresses LZ4-compressed data
func decompressLZ4(data []byte) ([]byte, error) {
	if len(data) < len(COMPRESSION_HEADER)+8 {
		return data, nil // Not compressed
	}
	
	if string(data[:len(COMPRESSION_HEADER)]) != COMPRESSION_HEADER {
		return data, nil // Not compressed
	}
	
	offset := len(COMPRESSION_HEADER)
	var originalSize uint64
	if len(data) < offset+8 {
		return data, nil
	}
	originalSize = uint64(data[offset]) | uint64(data[offset+1])<<8 | uint64(data[offset+2])<<16 | uint64(data[offset+3])<<24 |
		uint64(data[offset+4])<<32 | uint64(data[offset+5])<<40 | uint64(data[offset+6])<<48 | uint64(data[offset+7])<<56
	
	compressed := data[offset+8:]
	decompressed := make([]byte, originalSize)
	
	n, err := lz4.UncompressBlock(compressed, decompressed)
	if err != nil {
		return data, err
	}
	
	return decompressed[:n], nil
}

