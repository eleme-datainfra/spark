/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.orc;


import org.apache.hadoop.hive.ql.io.orc.OrcProto;
import com.facebook.presto.orc.metadata.StripeInformation;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;

import javax.annotation.Nullable;
import java.util.List;

public class OrcUtils {

    public static List<StripeInformation> toStripeInformation(
        List<OrcProto.StripeInformation> types) {
        return ImmutableList.copyOf(Iterables.transform(types,
            new Function<OrcProto.StripeInformation, StripeInformation>() {
                @Nullable
                @Override
                public StripeInformation apply(
                    @Nullable OrcProto.StripeInformation stripeInformation) {
                    return toStripeInformation(stripeInformation);
                }
            }
        ));
    }

    private static StripeInformation toStripeInformation(
        OrcProto.StripeInformation stripeInformation)
    {
        return new StripeInformation(
            Ints.checkedCast(stripeInformation.getNumberOfRows()),
            stripeInformation.getOffset(),
            stripeInformation.getIndexLength(),
            stripeInformation.getDataLength(),
            stripeInformation.getFooterLength());
    }
}
