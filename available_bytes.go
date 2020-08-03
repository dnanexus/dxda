// +build !windows

package dxda

import "syscall"

func getAvailableBytes(wd string) (int64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(wd, &stat); err != nil {
		return 0, err
	}
	// Available blocks * size per block = available space in bytes
	availableBytes := int64(stat.Bavail) * int64(stat.Bsize)
	return availableBytes, nil
}
