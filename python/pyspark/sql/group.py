#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from pyspark.rdd import ignore_unicode_prefix
from pyspark.sql import since
from pyspark.sql.column import Column, _to_seq
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import *

__all__ = ["GroupedData"]


def dfapi(f):
    def _api(self):
        name = f.__name__
        jdf = getattr(self._jdf, name)()
        return DataFrame(jdf, self.sql_ctx)
    _api.__name__ = f.__name__
    _api.__doc__ = f.__doc__
    return _api


def df_varargs_api(f):
    def _api(self, *args):
        name = f.__name__
        jdf = getattr(self._jdf, name)(_to_seq(self.sql_ctx._sc, args))
        return DataFrame(jdf, self.sql_ctx)
    _api.__name__ = f.__name__
    _api.__doc__ = f.__doc__
    return _api


class GroupedData(object):
    """
    A set of methods for aggregations on a :class:`DataFrame`,
    created by :func:`DataFrame.groupBy`.

    .. note:: Experimental

    .. versionadded:: 1.3
    """

    def __init__(self, jdf, sql_ctx):
        self._jdf = jdf
        self.sql_ctx = sql_ctx

    @ignore_unicode_prefix
    @since(1.3)
    def agg(self, *exprs):
        """Compute aggregates and returns the result as a :class:`DataFrame`.

        The available aggregate functions are `avg`, `max`, `min`, `sum`, `count`.

        If ``exprs`` is a single :class:`dict` mapping from string to string, then the key
        is the column to perform aggregation on, and the value is the aggregate function.

        Alternatively, ``exprs`` can also be a list of aggregate :class:`Column` expressions.

        :param exprs: a dict mapping from column name (string) to aggregate functions (string),
            or a list of :class:`Column`.

        >>> gdf = df.groupBy(df.name)
        >>> gdf.agg({"*": "count"}).collect()
        [Row(name=u'Alice', COUNT(1)=1), Row(name=u'Bob', COUNT(1)=1)]

        >>> from pyspark.sql import functions as F
        >>> gdf.agg(F.min(df.age)).collect()
        [Row(name=u'Alice', MIN(age)=2), Row(name=u'Bob', MIN(age)=5)]
        """
        assert exprs, "exprs should not be empty"
        if len(exprs) == 1 and isinstance(exprs[0], dict):
            jdf = self._jdf.agg(exprs[0])
        else:
            # Columns
            assert all(isinstance(c, Column) for c in exprs), "all exprs should be Column"
            jdf = self._jdf.agg(exprs[0]._jc,
                                _to_seq(self.sql_ctx._sc, [c._jc for c in exprs[1:]]))
        return DataFrame(jdf, self.sql_ctx)

    @dfapi
    @since(1.3)
    def count(self):
        """Counts the number of records for each group.

        >>> df.groupBy(df.age).count().collect()
        [Row(age=2, count=1), Row(age=5, count=1)]
        """

    @df_varargs_api
    @since(1.3)
    def mean(self, *cols):
        """Computes average values for each numeric columns for each group.

        :func:`mean` is an alias for :func:`avg`.

        :param cols: list of column names (string). Non-numeric columns are ignored.

        >>> df.groupBy().mean('age').collect()
        [Row(AVG(age)=3.5)]
        >>> df3.groupBy().mean('age', 'height').collect()
        [Row(AVG(age)=3.5, AVG(height)=82.5)]
        """

    @df_varargs_api
    @since(1.3)
    def avg(self, *cols):
        """Computes average values for each numeric columns for each group.

        :func:`mean` is an alias for :func:`avg`.

        :param cols: list of column names (string). Non-numeric columns are ignored.

        >>> df.groupBy().avg('age').collect()
        [Row(AVG(age)=3.5)]
        >>> df3.groupBy().avg('age', 'height').collect()
        [Row(AVG(age)=3.5, AVG(height)=82.5)]
        """

    @df_varargs_api
    @since(1.3)
    def max(self, *cols):
        """Computes the max value for each numeric columns for each group.

        >>> df.groupBy().max('age').collect()
        [Row(MAX(age)=5)]
        >>> df3.groupBy().max('age', 'height').collect()
        [Row(MAX(age)=5, MAX(height)=85)]
        """

    @df_varargs_api
    @since(1.3)
    def min(self, *cols):
        """Computes the min value for each numeric column for each group.

        :param cols: list of column names (string). Non-numeric columns are ignored.

        >>> df.groupBy().min('age').collect()
        [Row(MIN(age)=2)]
        >>> df3.groupBy().min('age', 'height').collect()
        [Row(MIN(age)=2, MIN(height)=80)]
        """

    @df_varargs_api
    @since(1.3)
    def sum(self, *cols):
        """Compute the sum for each numeric columns for each group.

        :param cols: list of column names (string). Non-numeric columns are ignored.

        >>> df.groupBy().sum('age').collect()
        [Row(SUM(age)=7)]
        >>> df3.groupBy().sum('age', 'height').collect()
        [Row(SUM(age)=7, SUM(height)=165)]
        """


def _test():
    import doctest
    from pyspark.context import SparkContext
    from pyspark.sql import Row, SQLContext
    import pyspark.sql.group
    globs = pyspark.sql.group.__dict__.copy()
    sc = SparkContext('local[4]', 'PythonTest')
    globs['sc'] = sc
    globs['sqlContext'] = SQLContext(sc)
    globs['df'] = sc.parallelize([(2, 'Alice'), (5, 'Bob')]) \
        .toDF(StructType([StructField('age', IntegerType()),
                          StructField('name', StringType())]))
    globs['df3'] = sc.parallelize([Row(name='Alice', age=2, height=80),
                                   Row(name='Bob', age=5, height=85)]).toDF()

    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.group, globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE | doctest.REPORT_NDIFF)
    globs['sc'].stop()
    if failure_count:
        exit(-1)


if __name__ == "__main__":
    _test()
