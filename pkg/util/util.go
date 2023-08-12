package util

func Every[T any](s []T, comp func(T) (bool, error)) (bool, error) {
	exists := true
	for _, data := range s {
		if valid, err := comp(data); err == nil {
			exists = valid && exists
		} else {
			return false, err
		}
	}

	return exists, nil
}

func Some[T any](s []T, comp func(T) (bool, error)) (bool, error) {
	exists := false
	for _, data := range s {
		if valid, err := comp(data); err == nil {
			exists = valid || exists
		} else {
			return false, err
		}
	}

	return exists, nil
}
