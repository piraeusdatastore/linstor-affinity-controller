package controller

import (
	"github.com/LINBIT/golinstor/client"
	"k8s.io/klog/v2"
)

type klogW struct {
	klog.Verbose
}

func (k klogW) Printf(s string, i ...interface{}) {
	k.Infof(s, i...)
}

func KLogV(level klog.Level) client.Logger {
	return &klogW{Verbose: klog.V(level)}
}
