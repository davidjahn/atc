package gcng_test

import (
	"github.com/concourse/atc/dbng"
	"github.com/concourse/atc/gcng"

	"code.cloudfoundry.org/lager/lagertest"
	"github.com/concourse/atc/dbng/dbngfakes"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("ContainerCollector", func() {

	var (
		workerProvider *dbngfakes.FakeWorkerFactory
		logger         lagertest.TestLogger

		c *gcng.ContainerCollector
	)

	var (
		dbConn           dbng.Conn
		containerFactory *dbng.ContainerFactory
	)

	BeforeEach(func() {
		postgresRunner.Truncate()

		dbConn = dbng.Wrap(postgresRunner.Open())
		containerFactory = dbng.NewContainerFactory(dbConn)
		workerProvider = dbng.NewWorkerFactory(dbConn)
		logger = lagertest.NewTestLogger("test")

		c = &gcng.ContainerCollector{
			Logger:            logger,
			ContainerProvider: containerFactory,
			WorkerProvider:    workerProvider,
		}
	})

	Describe("Run", func() {
		var (
			err error
		)

		BeforeEach(func() {

		})

		JustBeforeEach(func() {
			err = c.Run()
		})

		It("marks build containers for deletion", func() {
		})

		It("finds all containers in deleting state", func() {
		})

		Context("when deleting containers are found", func() {

			BeforeEach(func() {
				fakeContainerProvider.FindContainersMarkedForDeletionReturns(deletingContainers, nil)
			})

			Context("given a garden client for the worker can be found", func() {
				It("for each container it tells the garden client to delete it", func() {
					Expect(fakeGardenClient.DestroyCallCount()).To(Equal(1))
				})

				Context("when the container is removed by garden", func() {
					It("removes the container from the db", func() {

					})
				})

				Context("when removing the container from garden fails", func() {
					It("doesn't remove the container from the db", func() {

					})
				})

			})

			Context("when the worker cannot be found", func() {
				// some stuff happens to the worker maybe?
				// or nothing..
			})
		})

	})

})
