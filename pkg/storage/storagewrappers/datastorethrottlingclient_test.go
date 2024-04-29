package storagewrappers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/telemetry"
)

func TestDatastoreThrottlingClient(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	t.Run("read_usertuple_below_threshold_default_threshold", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockRelationshipTupleReader := mocks.NewMockRelationshipTupleReader(ctrl)
		mockThrottler := mocks.NewMockThrottler(ctrl)

		dut := NewDatastoreThrottlingClient(mockRelationshipTupleReader, 100, 120, mockThrottler)
		ctx := context.Background()

		mockRelationshipTupleReader.EXPECT().ReadUserTuple(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
		_, err := dut.ReadUserTuple(ctx, "abc", nil)
		require.NoError(t, err)

		// assert we did not throttle as this is the first call
		require.False(t, dut.HadThrottled())
		require.Equal(t, uint32(1), dut.readCount.Load())
	})

	t.Run("read_usertuple_use_per_flow_threshold", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockRelationshipTupleReader := mocks.NewMockRelationshipTupleReader(ctrl)
		mockThrottler := mocks.NewMockThrottler(ctrl)

		dut := NewDatastoreThrottlingClient(mockRelationshipTupleReader, 100, 120, mockThrottler)
		dut.readCount.Swap(105)
		ctx := context.Background()
		ctx = telemetry.ContextWithDatastoreThrottlingThreshold(ctx, 110)

		mockRelationshipTupleReader.EXPECT().ReadUserTuple(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
		_, err := dut.ReadUserTuple(ctx, "abc", nil)
		require.NoError(t, err)

		// assert we did not throttle as this is the first call
		require.False(t, dut.HadThrottled())
		require.Equal(t, uint32(106), dut.readCount.Load())
	})

	t.Run("read_usertuple_throttled", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockRelationshipTupleReader := mocks.NewMockRelationshipTupleReader(ctrl)
		mockThrottler := mocks.NewMockThrottler(ctrl)

		mockThrottler.EXPECT().Throttle(gomock.Any()).Times(1)
		dut := NewDatastoreThrottlingClient(mockRelationshipTupleReader, 100, 0, mockThrottler)
		dut.readCount.Swap(105)
		ctx := context.Background()

		mockRelationshipTupleReader.EXPECT().ReadUserTuple(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
		_, err := dut.ReadUserTuple(ctx, "abc", nil)
		require.NoError(t, err)

		// assert we did not throttle as this is the first call
		require.True(t, dut.HadThrottled())
		require.Equal(t, uint32(106), dut.readCount.Load())
	})

	t.Run("read_usertuple_throttled_respect_max", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockRelationshipTupleReader := mocks.NewMockRelationshipTupleReader(ctrl)
		mockThrottler := mocks.NewMockThrottler(ctrl)

		mockThrottler.EXPECT().Throttle(gomock.Any()).Times(1)
		dut := NewDatastoreThrottlingClient(mockRelationshipTupleReader, 100, 0, mockThrottler)
		dut.readCount.Swap(105)

		ctx := context.Background()
		ctx = telemetry.ContextWithDatastoreThrottlingThreshold(ctx, 110)

		mockRelationshipTupleReader.EXPECT().ReadUserTuple(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
		_, err := dut.ReadUserTuple(ctx, "abc", nil)
		require.NoError(t, err)

		// assert we did not throttle as this is the first call
		require.True(t, dut.HadThrottled())
		require.Equal(t, uint32(106), dut.readCount.Load())
	})

	t.Run("read_throttled", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockRelationshipTupleReader := mocks.NewMockRelationshipTupleReader(ctrl)
		mockThrottler := mocks.NewMockThrottler(ctrl)

		mockThrottler.EXPECT().Throttle(gomock.Any()).Times(1)
		dut := NewDatastoreThrottlingClient(mockRelationshipTupleReader, 100, 0, mockThrottler)
		dut.readCount.Swap(105)
		ctx := context.Background()

		mockRelationshipTupleReader.EXPECT().Read(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
		_, err := dut.Read(ctx, "abc", nil)
		require.NoError(t, err)

		// assert we did not throttle as this is the first call
		require.True(t, dut.HadThrottled())
		require.Equal(t, uint32(106), dut.readCount.Load())
	})

	t.Run("read_userset_tuples_throttled", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockRelationshipTupleReader := mocks.NewMockRelationshipTupleReader(ctrl)
		mockThrottler := mocks.NewMockThrottler(ctrl)

		mockThrottler.EXPECT().Throttle(gomock.Any()).Times(1)
		dut := NewDatastoreThrottlingClient(mockRelationshipTupleReader, 100, 0, mockThrottler)
		dut.readCount.Swap(105)
		ctx := context.Background()

		mockRelationshipTupleReader.EXPECT().ReadUsersetTuples(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
		_, err := dut.ReadUsersetTuples(ctx, "abc", storage.ReadUsersetTuplesFilter{})
		require.NoError(t, err)

		// assert we did not throttle as this is the first call
		require.True(t, dut.HadThrottled())
		require.Equal(t, uint32(106), dut.readCount.Load())
	})
}
