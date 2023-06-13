/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import com.google.inject.Inject;
import com.starburstdata.presto.server.security.webui.access.WebUiAccessControl;
import com.starburstdata.presto.server.ui.WebSessionRequest;
import io.airlift.units.Duration;
import io.trino.server.security.ResourceSecurity;
import io.trino.spi.QueryId;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SelectedRole;
import jakarta.annotation.Nullable;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.transform;
import static io.starburst.server.troubleshooting.TroubleshootingCoordinatorResource.BASE_PATH_API_V1;
import static io.airlift.jaxrs.AsyncResponseHandler.bindAsyncResponse;
import static io.trino.server.security.ResourceSecurity.AccessType.WEB_UI;
import static jakarta.ws.rs.core.Response.Status.BAD_REQUEST;
import static jakarta.ws.rs.core.Response.Status.FORBIDDEN;
import static java.lang.String.format;
import static java.net.URLDecoder.decode;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

@ResourceSecurity(WEB_UI)
@Path(BASE_PATH_API_V1)
public class TroubleshootingCoordinatorResource
{
    private static final Duration MAX_POOL_TIME_MS = Duration.valueOf("5s");
    public static final String BASE_PATH_API_V1 = "/ui/troubleshooting";
    private final WebUiAccessControl accessControl;
    private final TroubleshootingManager troubleshootingManager;
    private final ScheduledExecutorService executorService;

    @Inject
    public TroubleshootingCoordinatorResource(WebUiAccessControl accessControl, TroubleshootingManager troubleshootingManager, @ForTroubleshooting ScheduledExecutorService executorService)
    {
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.troubleshootingManager = requireNonNull(troubleshootingManager, "troubleshootingManager is null");
        this.executorService = requireNonNull(executorService, "executorService is null");
    }

    @ResourceSecurity(WEB_UI)
    @GET
    public void getTroubleshootingArchive(@QueryParam("queryId") QueryId queryId, @QueryParam("selectedRole") String selectedRole, @Context WebSessionRequest webRequest, @Context ContainerRequestContext request, @Context @Suspended AsyncResponse asyncResponse)
    {
        assertRequest(queryId != null, "Query id was not provided");

        Identity identity = setSelectedRoleIfPresent(webRequest.getIdentity(), selectedRole);
        if (!accessControl.isPrivilegedUser(identity)) {
            bindAsyncResponse(asyncResponse, immediateFuture(Response.status(FORBIDDEN.getStatusCode(), "You are not allowed to download troubleshooting archive").build()), executorService);
            return;
        }

        bindAsyncResponse(asyncResponse, transform(troubleshootingManager.getInputStreams(queryId), streams -> createArchiveFromStreams(streams, queryId), executorService), executorService)
                .withTimeout(MAX_POOL_TIME_MS, retryPollingResponse(request));
    }

    private Identity setSelectedRoleIfPresent(Identity identity, @Nullable String roleQueryParam)
    {
        Optional<String> selectedRole = Optional.ofNullable(roleQueryParam)
                .flatMap(TroubleshootingCoordinatorResource::getSelectedRole);

        Identity.Builder identityBuilder = Identity.from(identity);
        selectedRole.ifPresent(role -> identityBuilder.withEnabledRoles(Set.of(role)));
        return identityBuilder.build();
    }

    private static Optional<String> getSelectedRole(String selectedRole)
    {
        return parseSelectedRole(selectedRole)
                .entrySet()
                .stream()
                .filter(entry -> entry.getKey().equalsIgnoreCase("system"))
                .map(Map.Entry::getValue)
                .map(SelectedRole::valueOf)
                .findFirst()
                .flatMap(SelectedRole::getRole);
    }

    private static Map<String, String> parseSelectedRole(String selectedRole)
    {
        Splitter splitter = Splitter.on(',').trimResults().omitEmptyStrings();
        Splitter keyValueSplitter = Splitter.on('=').trimResults();

        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (String role : splitter.splitToList(selectedRole)) {
            List<String> parts = keyValueSplitter.splitToList(role);
            assertRequest(parts.size() == 2, "Invalid role value: %s", role);

            try {
                builder.put(parts.get(0), decode(parts.get(1), UTF_8));
            }
            catch (IllegalArgumentException e) {
                throw new WebApplicationException("Invalid selected role: " + parts.get(1), BAD_REQUEST);
            }
        }

        return builder.buildOrThrow();
    }

    @FormatMethod
    private static void assertRequest(boolean expression, @FormatString String format, Object... args)
    {
        if (!expression) {
            throw new WebApplicationException(format(format, args), BAD_REQUEST);
        }
    }

    private Response createArchiveFromStreams(Map<String, InputStream> inputStream, QueryId queryId)
    {
        try (ByteArrayOutputStream output = new ByteArrayOutputStream(); ZipOutputStream archive = new ZipOutputStream(output)) {
            for (Map.Entry<String, InputStream> entry : inputStream.entrySet()) {
                archive.putNextEntry(new ZipEntry(queryId + "/" + entry.getKey()));
                archive.write(entry.getValue().readAllBytes());
                archive.closeEntry();
            }

            archive.flush();
            archive.finish();

            return Response.ok(new ByteArrayInputStream(output.toByteArray()))
                    .header("Content-Type", "application/zip")
                    .header("Content-disposition", "attachment; filename=\"starburst-query-troubleshooting-%s.zip\"".formatted(queryId))
                    .build();
        }
        catch (IOException e) {
            throw new WebApplicationException("Could not create troubleshooting archive: " + e.getMessage());
        }
    }

    private static Response retryPollingResponse(ContainerRequestContext request)
    {
        return Response.seeOther(request.getUriInfo().getRequestUri()).build();
    }
}
