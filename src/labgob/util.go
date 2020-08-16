package labgob

import "bytes"

// EncodeToByteArray 将给定的数据encode为byte数组
func EncodeToByteArray(data interface{}) []byte {
	writer := new(bytes.Buffer)
	encoder := NewEncoder(writer)
	err := encoder.Encode(data)
	if err != nil {
		panic(err)
	}
	return writer.Bytes()
}
