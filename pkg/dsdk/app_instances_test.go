package dsdk

import (
	"testing"

	"github.com/Datera/go-udc/pkg/udc"
	"github.com/sirupsen/logrus"
)

func TestAppInstances_ListCh(t *testing.T) {
	logrus.StandardLogger().SetLevel(logrus.ErrorLevel)
	cfg, err := udc.GetConfig()
	if err != nil {
		t.Fatal(err)
	}
	sdk, err := NewSDK(cfg, false)
	if err != nil {
		t.Fatal(err)
	}

	total := 0
	aiCh, aerCh, errCh := sdk.AppInstances.ListCh(&AppInstancesListRequest{Ctxt: sdk.NewContext()})
	for {
		select {
		case ai, ok := <-aiCh:
			if ai != nil {
				total += 1
				t.Log(ai.Name)
			}
			if !ok {
				aiCh = nil
			}
		case aer, ok := <-aerCh:
			if aer != nil {
				t.Fatal(aer)
			}
			if !ok {
				aerCh = nil
			}
		case err, ok := <-errCh:
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				errCh = nil
			}
		}
		if aiCh == nil && aerCh == nil && errCh == nil {
			break
		}
	}
	t.Log(total)

	ais, _, _ := sdk.AppInstances.List(&AppInstancesListRequest{Ctxt: sdk.NewContext()})
	t.Log(len(ais))
}
