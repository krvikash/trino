/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.plugin.openapi.pagination;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import com.google.inject.binder.LinkedBindingBuilder;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import static com.google.inject.Scopes.SINGLETON;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class PaginationStrategyModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        LinkedBindingBuilder<OpenApiPaginationStrategy<?>> strategyBinding =
                binder.bind(new TypeLiteral<>() {});

        (switch (buildConfigObject(PaginationConfig.class).getPaginationType()) {
            case NONE -> strategyBinding.to(ReadOnceStrategy.class);
            case OFFSET -> {
                configBinder(binder).bindConfig(OffsetPaginationConfig.class);
                yield strategyBinding.toProvider(OffsetPaginationStrategyProvider.class);
            }
            case PAGE_NUMBER -> {
                configBinder(binder).bindConfig(PageNumberPaginationConfig.class);
                yield strategyBinding.toProvider(PageNumberPaginationStrategyProvider.class);
            }
            case LINK_HEADER -> strategyBinding.toProvider(LinkHeaderPaginationStrategyProvider.class);
            case NEXT_CURSOR_FIELD -> {
                configBinder(binder).bindConfig(NextCursorFieldPaginationConfig.class);
                yield strategyBinding.toProvider(NextCursorFieldPaginationStrategyProvider.class);
            }
            case LAST_ELEMENT_CURSOR_FIELD -> {
                configBinder(binder).bindConfig(LastElementCursorFieldPaginationConfig.class);
                yield strategyBinding.toProvider(LastElementFieldCursorPaginationStrategyProvider.class);
            }
            case NEXT_URL_FIELD -> {
                configBinder(binder).bindConfig(NextUrlFieldPaginationConfig.class);
                yield strategyBinding.toProvider(NextUrlFieldPaginationStrategyProvider.class);
            }
        }).in(SINGLETON);
    }

    public static class OffsetPaginationStrategyProvider
            implements Provider<OpenApiPaginationStrategy<?>>
    {
        private final OffsetPaginationConfig config;

        @Inject
        public OffsetPaginationStrategyProvider(OffsetPaginationConfig config)
        {
            this.config = requireNonNull(config, "config is null");
        }

        @Override
        public OpenApiPaginationStrategy<?> get()
        {
            return new OffsetPaginationStrategy(
                    config.getOffsetParameterName(),
                    config.getDataFieldJsonPointer());
        }
    }

    public static class PageNumberPaginationStrategyProvider
            implements Provider<OpenApiPaginationStrategy<?>>
    {
        private final PageNumberPaginationConfig config;

        @Inject
        public PageNumberPaginationStrategyProvider(PageNumberPaginationConfig config)
        {
            this.config = requireNonNull(config, "config is null");
        }

        @Override
        public OpenApiPaginationStrategy<?> get()
        {
            return new PageNumberPaginationStrategy(
                    config.getPageParameterName(),
                    config.getIsLastPageFieldJsonPointer());
        }
    }

    public static class LinkHeaderPaginationStrategyProvider
            implements Provider<OpenApiPaginationStrategy<?>>
    {
        @Override
        public OpenApiPaginationStrategy<?> get()
        {
            return new LinkHeaderPaginationStrategy();
        }
    }

    public static class NextCursorFieldPaginationStrategyProvider
            implements Provider<OpenApiPaginationStrategy<?>>
    {
        private final NextCursorFieldPaginationConfig config;

        @Inject
        public NextCursorFieldPaginationStrategyProvider(NextCursorFieldPaginationConfig config)
        {
            this.config = requireNonNull(config, "config is null");
        }

        @Override
        public OpenApiPaginationStrategy<?> get()
        {
            return new NextCursorFieldPaginationStrategy(
                    config.getCursorParameterName(),
                    config.getCursorFieldJsonPointer());
        }
    }

    public static class LastElementFieldCursorPaginationStrategyProvider
            implements Provider<OpenApiPaginationStrategy<?>>
    {
        private final LastElementCursorFieldPaginationConfig config;

        @Inject
        public LastElementFieldCursorPaginationStrategyProvider(LastElementCursorFieldPaginationConfig config)
        {
            this.config = requireNonNull(config, "config is null");
        }

        @Override
        public OpenApiPaginationStrategy<?> get()
        {
            return new LastElementCursorFieldPaginationStrategy(
                    config.getCursorParameterName(),
                    config.getDataFieldJsonPointer(),
                    config.getCursorFieldJsonPointer());
        }
    }

    public static class NextUrlFieldPaginationStrategyProvider
            implements Provider<OpenApiPaginationStrategy<?>>
    {
        private final NextUrlFieldPaginationConfig config;

        @Inject
        public NextUrlFieldPaginationStrategyProvider(NextUrlFieldPaginationConfig config)
        {
            this.config = requireNonNull(config, "config is null");
        }

        @Override
        public OpenApiPaginationStrategy<?> get()
        {
            return new NextUrlFieldPaginationStrategy(config.getNextUrlFieldJsonPointer());
        }
    }
}
