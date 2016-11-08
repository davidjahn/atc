package gcng

import (
	"code.cloudfoundry.org/lager"
	"github.com/concourse/atc/db"
)

//go:generate counterfeiter . DBClient

type DBClient interface {
	ContainersBelongingToFinishedNonFailedBuilds() ([]db.SavedContainer, error)
}


type Containercollector struct{
	Logger lager.Logger
	DB DBClient
}

func (c *Containercollector) Run() error {
	_, err := c.DB.ContainersBelongingToFinishedNonFailedBuilds()
	if err != nil {
		c.Logger.Error("failed-to-find-containers-beloging-to-finished-nonfailing-builds", err)
		return err
	}



	return nil
}