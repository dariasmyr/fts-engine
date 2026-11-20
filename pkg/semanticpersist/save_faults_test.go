package semanticpersist

type saveFaultInjector struct {
	beforeStep func(saveCheckpoint) error
	afterStep  func(saveCheckpoint) error
}

func (i saveFaultInjector) before(checkpoint saveCheckpoint) error {
	if i.beforeStep == nil {
		return nil
	}
	return i.beforeStep(checkpoint)
}

func (i saveFaultInjector) after(checkpoint saveCheckpoint) error {
	if i.afterStep == nil {
		return nil
	}
	return i.afterStep(checkpoint)
}
