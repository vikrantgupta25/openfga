package storagewrappers

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/tuple"
)

func TestDelay(t *testing.T) {
	threshold := 0
	delayedReader := NewDelayedTupleReader(nil, uint64(threshold))

	delays := map[int]time.Duration{}

	ticker := time.NewTicker(1e6) // 1ms

	for i := 0; i < 100; i++ {
		nowStart := <-ticker.C
		delayedReader.delay()
		nowEnd := <-ticker.C

		delays[i] = nowEnd.Sub(nowStart)
	}

	for i := 0; i < len(delays)-1; i++ {
		if i > threshold {
			require.GreaterOrEqual(t, delays[i+1].Milliseconds(), delays[i].Milliseconds())
		}
	}

	require.Greater(t, delays[len(delays)-1], delays[0])
}

func TestDelayedTupleReader_Read(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	threshold := 50
	mockedReader := mocks.NewMockRelationshipTupleReader(mockController)
	delayedReader := NewDelayedTupleReader(nil, uint64(threshold))

	mockedReader.EXPECT().
		Read(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil).
		AnyTimes()

	delays := map[int]time.Duration{}

	ctx := context.Background()
	storeID := ulid.Make().String()
	tk := tuple.NewTupleKey("document:1", "viewer", "user:jon")

	for i := 0; i < 100; i++ {
		timerStart := time.Now()
		_, err := delayedReader.Read(ctx, storeID, tk)
		require.NoError(t, err)

		delays[i] = time.Since(timerStart)

		if i > threshold {
			log.Printf("delays[%d] = %s\n", i, delays[i])
		}
	}

	for i := 0; i < len(delays)-10; i++ {
		if i > threshold {
			require.GreaterOrEqual(t, delays[i+10], delays[i])
		}
	}
}
