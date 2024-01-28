/*
Copyright (c) Meta Platforms, Inc. and affiliates.
This source code is licensed under the MIT license found in the
LICENSE file in the root directory of this source tree.
*/

package moqorigins

type MoqOriginsData struct {
	MoqOrigins []MoqOriginData `json:"origins"`
}

type MoqOrigins struct {
	moqOriginsInfo []moqOriginExt
}

// New Creates a new moq origins list
func New() *MoqOrigins {
	mos := MoqOrigins{}
	return &mos
}

func (mors *MoqOrigins) Initialize(moqOriginsData MoqOriginsData) (err error) {
	for _, moqOriginData := range moqOriginsData.MoqOrigins {
		or := newOrigin(moqOriginData)
		mors.moqOriginsInfo = append(mors.moqOriginsInfo, moqOriginExt{moqOriginData, or})
	}
	return
}

func (mors *MoqOrigins) Close() (err error) {
	for _, moqOrExt := range mors.moqOriginsInfo {
		moqOrExt.moqOriginPtr.Close()
	}
	return
}

func (mors *MoqOrigins) ToString() string {
	str := ""
	for i, moqOrExt := range mors.moqOriginsInfo {
		if i > 0 {
			str = str + ","
		}
		str = str + moqOrExt.FriendlyName
	}
	return str
}
