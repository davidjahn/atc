package gcng

import (
	"errors"

	"code.cloudfoundry.org/garden"
	"code.cloudfoundry.org/garden/client"
	"code.cloudfoundry.org/garden/client/connection"
	"code.cloudfoundry.org/lager"
	"github.com/concourse/atc/dbng"
)

type ContainerCollector struct {
	Logger            lager.Logger
	ContainerProvider dbng.ContainerFactory
	WorkerProvider    dbng.WorkerFactory
}

type GardenClientFactory func(*dbng.Worker) (garden.Client, error)

func NewGardenClientFactory() GardenClientFactory {
	return func(w *dbng.Worker) (garden.Client, error) {

	}
}

func (c *ContainerCollector) Run() error {
	// get containers that should be marked for deletion
	// --> not used in last failed build of a job
	// --> used for a resource that doesn't exist
	// --> part of a pipeline that doesn't exist
	err := c.ContainerProvider.MarkBuildContainersForDeletion()
	if err != nil {
		return err
	}

	cs, err := c.ContainerProvider.FindContainersMarkedForDeletion()
	if err != nil {
		return err
	}

	for _, container := range cs {
		w, found, err := c.WorkerProvider.GetWorker(container.WorkerName)
		if err != nil {
			continue
		}

		if !found {
			continue
		}

		if w.GardenAddr == nil {
			continue
		}

		gconn := connection.New("tcp", *w.GardenAddr)
		gclient := client.New(gconn)

		gclient.Destroy(container.Handle)
	}

	// mark those containers for deletion
	// find containers marked for deletion

	// FindContainersMarkedForDeletion
	// try and delete from garden
	// create client for the worker the container is on
	// gardenClient.Delete(handle)

	// once deleting from garden succeeds delete from db

	return nil
}
