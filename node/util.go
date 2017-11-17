package node

import "crypto/md5"

func md5Hash(key string) []byte {
	md := md5.New()
	md.Write([]byte(key))
	return md.Sum(nil)
}

func kemataHash(key string, i int) uint32 {
	digest := md5Hash(key)
	// calculate the hash value
	// each four bytes constitute a 32-bit integer
	// then add the four 32-bit integers to the final hash value
	var hash uint32 = 0
	hash += (uint32(digest[(i << 2) + 3] & 0xff) << 24) |
		(uint32(digest[(i << 2) + 2] & 0xff) << 16) |
		(uint32(digest[(i << 2) + 1] & 0xff) << 8) |
		uint32(digest[i << 2] & 0xff)

	return hash
}