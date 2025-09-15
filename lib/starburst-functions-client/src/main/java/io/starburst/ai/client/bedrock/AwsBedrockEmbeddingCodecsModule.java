/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client.bedrock;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static com.google.inject.multibindings.MapBinder.newMapBinder;

public class AwsBedrockEmbeddingCodecsModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        var codecBinder = newMapBinder(binder, String.class, AwsEmbeddingCodec.Factory.class);
        codecBinder.addBinding(TitanTextV2Codec.MODEL_NAME).to(TitanTextV2Codec.Factory.class).in(Scopes.SINGLETON);
        codecBinder.addBinding(CohereEmbedMultilingualV3Codec.MODEL_NAME).to(CohereEmbedMultilingualV3Codec.Factory.class).in(Scopes.SINGLETON);
    }
}
