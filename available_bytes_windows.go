// +build windows

package dxda

import (
	"syscall"
	"unsafe"
)

func getAvailableBytes(wd string) (int64, error) {
	h := syscall.MustLoadDLL("kernel32.dll")
	c := h.MustFindProc("GetDiskFreeSpaceExW")

	var availableBytes int64
	var totalBytes int64
	var freeBytes int64

	c.Call(
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(wd))),
		uintptr(unsafe.Pointer(&freeBytes)),
		uintptr(unsafe.Pointer(&totalBytes)),
		uintptr(unsafe.Pointer(&availableBytes)))

	return availableBytes, nil
}
