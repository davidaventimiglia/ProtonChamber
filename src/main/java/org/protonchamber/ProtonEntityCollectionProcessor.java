package org.protonchamber;

import java.io.*;
import java.util.*;
import org.apache.olingo.commons.api.data.*;
import org.apache.olingo.commons.api.edm.*;
import org.apache.olingo.commons.api.format.*;
import org.apache.olingo.commons.api.http.*;
import org.apache.olingo.server.api.*;
import org.apache.olingo.server.api.processor.*;
import org.apache.olingo.server.api.serializer.*;
import org.apache.olingo.server.api.uri.*;

public class ProtonEntityCollectionProcessor implements EntityCollectionProcessor {
    OData odata;
    ServiceMetadata serviceMetaData;

    @Override
    public void init (OData odata, ServiceMetadata serviceMetaData) {
	this.odata = odata;
	this.serviceMetaData = serviceMetaData;
    }

    @Override
    public void readEntityCollection (ODataRequest request,
				      ODataResponse response,
				      UriInfo uriInfo,
				      ContentType responseFormat)
	throws ODataApplicationException,
	       ODataLibraryException {
	List<UriResource> resourcePaths = uriInfo.getUriResourceParts();
	UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet) resourcePaths.get(0);
	EdmEntitySet edmEntitySet = uriResourceEntitySet.getEntitySet();
	EntityCollection entitySet = new EntityCollection();
	ODataSerializer serializer = odata.createSerializer(responseFormat);
	EdmEntityType edmEntityType = edmEntitySet.getEntityType();
	ContextURL contextUrl = ContextURL.with().entitySet(edmEntitySet).build();
	final String id = request.getRawBaseUri() + "/" + edmEntitySet.getName();
	EntityCollectionSerializerOptions opts = EntityCollectionSerializerOptions.with().id(id).contextURL(contextUrl).build();
	SerializerResult serializerResult = serializer.entityCollection(serviceMetaData, edmEntityType, entitySet, opts);
	InputStream serializedContent = serializerResult.getContent();
	response.setContent(serializedContent);
	response.setStatusCode(HttpStatusCode.OK.getStatusCode());
	response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
    }
}
