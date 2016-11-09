package workerserver

import "net/http"

func (s *Server) LandWorker(w http.ResponseWriter, r *http.Request) {
	hLog := s.logger.Session("landing-worker")
  workerName := r.FormValue(":worker_name")

  _, found, err := s.WorkerFactory.GetWorker(workerName)
  if err != nil {
    logger.Error("failed-to-find-worker-to-land", err)
    w.WriteHeader(http.StatusInternalServerError)
    return
  }

  if found != true {
    logger.Error("worker-to-land-does-not-exist", err)
    w.WriteHeader(http.StatusInternalServerError)
    return
  }

  err = s.WorkerFactory.LandWorker(workerName)
  if err != nil {
    logger.Error("failed-to-land-worker", err)
    w.WriteHeader(http.StatusInternalServerError)
    return
  }
}
