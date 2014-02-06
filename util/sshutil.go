package util

import (
	"code.google.com/p/go.crypto/ssh"
	"fmt"
	"io"
	"io/ioutil"
)

type keychain struct {
	keys []ssh.Signer
}

func (k *keychain) Key(i int) (ssh.PublicKey, error) {
	if i != 0 {
		return nil, nil
	}
	return k.keys[i].PublicKey(), nil
}

func (k *keychain) Sign(i int, rand io.Reader, data []byte) (sig []byte, err error) {
	return k.keys[i].Sign(rand, data)
}

func (k *keychain) add(key ssh.Signer) {
	k.keys = append(k.keys, key)
}

func (k *keychain) loadPEM(file string) error {
	buf, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}
	key, err := ssh.ParsePrivateKey(buf)
	if err != nil {
		return err
	}
	k.add(key)
	return nil
}

func GetSSHConn(server string, username string, pemFile string) *ssh.ClientConn {

	clientKeychain := new(keychain)
	err := clientKeychain.loadPEM(pemFile)

	if err != nil {
		panic("Cannot load PEM File: " + err.Error())
	}

	config := &ssh.ClientConfig{
		User: username,
		Auth: []ssh.ClientAuth{
			ssh.ClientAuthKeyring(clientKeychain),
		},
	}
	client, err := ssh.Dial("tcp", fmt.Sprintf("%s:22", server), config)
	if err != nil {
		panic(err)
	}
	return client

}
