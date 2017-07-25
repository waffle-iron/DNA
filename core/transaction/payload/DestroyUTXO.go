package payload

import "io"

type DestroyUTXO struct {
}

func (a *DestroyUTXO) Data() []byte {
	//TODO: implement TransferAsset.Data()
	return []byte{0}

}

func (a *DestroyUTXO) Serialize(w io.Writer) error {
	return nil
}

func (a *DestroyUTXO) Deserialize(r io.Reader) error {
	return nil
}
