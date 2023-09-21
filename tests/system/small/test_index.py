# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import numpy
import pandas as pd

from tests.system.utils import assert_pandas_index_equal_ignore_index_type


def test_get_index(scalars_df_index, scalars_pandas_df_index):
    index = scalars_df_index.index
    bf_result = index.to_pandas()
    pd_result = scalars_pandas_df_index.index

    assert_pandas_index_equal_ignore_index_type(bf_result, pd_result)


def test_index_has_duplicates(scalars_df_index, scalars_pandas_df_index):
    bf_result = scalars_df_index.set_index("int64_col").index.has_duplicates
    pd_result = scalars_pandas_df_index.set_index("int64_col").index.has_duplicates
    assert bf_result == pd_result


def test_index_values(scalars_df_index, scalars_pandas_df_index):
    bf_result = scalars_df_index.index.values
    pd_result = scalars_pandas_df_index.index.values

    # Numpy isn't equipped to compare non-numeric objects, so convert back to dataframe
    pd.testing.assert_series_equal(
        pd.Series(bf_result), pd.Series(pd_result), check_dtype=False
    )


def test_index_ndim(scalars_df_index, scalars_pandas_df_index):
    bf_result = scalars_df_index.index.ndim
    pd_result = scalars_pandas_df_index.index.ndim

    assert pd_result == bf_result


def test_index_dtype(scalars_df_index, scalars_pandas_df_index):
    bf_result = scalars_df_index.index.dtype
    pd_result = scalars_pandas_df_index.index.dtype

    assert pd_result == bf_result


def test_index_dtypes(scalars_df_index, scalars_pandas_df_index):
    bf_result = scalars_df_index.set_index(["string_col", "int64_too"]).index.dtypes
    pd_result = scalars_pandas_df_index.set_index(
        ["string_col", "int64_too"]
    ).index.dtypes
    pd.testing.assert_series_equal(bf_result, pd_result)


def test_index_shape(scalars_df_index, scalars_pandas_df_index):
    bf_result = scalars_df_index.index.shape
    pd_result = scalars_pandas_df_index.index.shape

    assert bf_result == pd_result


def test_index_astype(scalars_df_index, scalars_pandas_df_index):
    bf_result = (
        scalars_df_index.set_index("int64_col").index.astype("Float64").to_pandas()
    )
    pd_result = scalars_pandas_df_index.set_index("int64_col").index.astype("Float64")
    pd.testing.assert_index_equal(bf_result, pd_result)


def test_index_any(scalars_df_index, scalars_pandas_df_index):
    bf_result = scalars_df_index.set_index("int64_col").index.any()
    pd_result = scalars_pandas_df_index.set_index("int64_col").index.any()
    assert bf_result == pd_result


def test_index_all(scalars_df_index, scalars_pandas_df_index):
    bf_result = scalars_df_index.set_index("int64_col").index.all()
    pd_result = scalars_pandas_df_index.set_index("int64_col").index.all()
    assert bf_result == pd_result


def test_index_max(scalars_df_index, scalars_pandas_df_index):
    bf_result = scalars_df_index.set_index("int64_col").index.max()
    pd_result = scalars_pandas_df_index.set_index("int64_col").index.max()
    assert bf_result == pd_result


def test_index_min(scalars_df_index, scalars_pandas_df_index):
    bf_result = scalars_df_index.set_index("int64_col").index.min()
    pd_result = scalars_pandas_df_index.set_index("int64_col").index.min()
    assert bf_result == pd_result


def test_index_nunique(scalars_df_index, scalars_pandas_df_index):
    bf_result = scalars_df_index.set_index("int64_col").index.nunique()
    pd_result = scalars_pandas_df_index.set_index("int64_col").index.nunique()
    assert bf_result == pd_result


def test_index_fillna(scalars_df_index, scalars_pandas_df_index):
    bf_result = scalars_df_index.set_index("int64_col").index.fillna(42).to_pandas()
    pd_result = scalars_pandas_df_index.set_index("int64_col").index.fillna(42)

    pd.testing.assert_index_equal(bf_result, pd_result)


def test_index_drop(scalars_df_index, scalars_pandas_df_index):
    bf_result = (
        scalars_df_index.set_index("int64_col").index.drop([2, 314159]).to_pandas()
    )
    pd_result = scalars_pandas_df_index.set_index("int64_col").index.drop([2, 314159])
    pd.testing.assert_index_equal(bf_result, pd_result)


def test_index_rename(scalars_df_index, scalars_pandas_df_index):
    bf_result = scalars_df_index.set_index("int64_col").index.rename("name").to_pandas()
    pd_result = scalars_pandas_df_index.set_index("int64_col").index.rename("name")
    pd.testing.assert_index_equal(bf_result, pd_result)


def test_index_multi_rename(scalars_df_index, scalars_pandas_df_index):
    bf_result = (
        scalars_df_index.set_index(["int64_col", "int64_too"])
        .index.rename(["new", "names"])
        .to_pandas()
    )
    pd_result = scalars_pandas_df_index.set_index(
        ["int64_col", "int64_too"]
    ).index.rename(["new", "names"])
    pd.testing.assert_index_equal(bf_result, pd_result)


def test_index_len(scalars_df_index, scalars_pandas_df_index):
    bf_result = len(scalars_df_index.index)
    pd_result = len(scalars_pandas_df_index.index)

    assert bf_result == pd_result


def test_index_array(scalars_df_index, scalars_pandas_df_index):
    bf_result = scalars_df_index.index.__array__()
    pd_result = scalars_pandas_df_index.index.__array__()

    numpy.array_equal(bf_result, pd_result)


def test_index_getitem_int(scalars_df_index, scalars_pandas_df_index):
    bf_result = scalars_df_index.index[-2]
    pd_result = scalars_pandas_df_index.index[-2]
    assert bf_result == pd_result


def test_is_monotonic_increasing(scalars_df_index, scalars_pandas_df_index):
    assert (
        scalars_df_index.index.is_monotonic_increasing
        == scalars_pandas_df_index.index.is_monotonic_increasing
    )


def test_is_monotonic_decreasing(scalars_df_index, scalars_pandas_df_index):
    assert (
        scalars_df_index.index.is_monotonic_increasing
        == scalars_pandas_df_index.index.is_monotonic_increasing
    )
