package gcng_test

import (
	"errors"

	"github.com/concourse/atc/gcng"

	"code.cloudfoundry.org/lager/lagertest"
	"github.com/concourse/atc/gcng/gcngfakes"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = FDescribe("Containercollector", func() {
	var (
		c            *gcng.Containercollector
		fakeDBClient *gcngfakes.FakeDBClient
		logger       *lagertest.TestLogger
	)

	BeforeEach(func() {
		fakeDBClient = new(gcngfakes.FakeDBClient)
		logger = lagertest.NewTestLogger("containercollector")

		c = &gcng.Containercollector{
			DB:     fakeDBClient,
			Logger: logger,
		}
	})

	Describe("Run", func() {
		It("finds containers belonging to builds that are no longer running (except for most recent builds, if they failed) and destroys them", func() {
			err := c.Run()
			Expect(err).NotTo(HaveOccurred())

			Expect(fakeDBClient.ContainersBelongingToFinishedNonFailedBuildsCallCount()).To(Equal(1))
		})

		It("errors when finding containers to destroy errors", func() {
			containerErr := errors.New("cant-find-containers")
			fakeDBClient.ContainersBelongingToFinishedNonFailedBuildsReturns(nil, containerErr)

			err := c.Run()
			Expect(err).To(MatchError(containerErr))
		})
	})
})
