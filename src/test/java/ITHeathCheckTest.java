package org.protonchamber;

import static org.junit.jupiter.api.Assertions.*;


import java.io.*;
import java.net.*;
import java.net.http.*;
import org.junit.jupiter.api.*;

class ITHealthCheckTest {
    @Test
    void myFirstTest() {
        assertEquals(2, 1 + 1);}


    @Test
    public void givenUserDoesNotExists_whenUserInfoIsRetrieved_then404IsReceived () throws URISyntaxException, IOException, InterruptedException {
	// Given
	String name = "abcdefg";
	HttpRequest request = HttpRequest.newBuilder()
	    .uri(new URI("https://api.github.com/users/" + name))
	    .GET()
	    .build();
 
	// When
	HttpResponse<String> response = HttpClient.newBuilder()
	    .proxy(ProxySelector.getDefault())
	    .build()
	    .send(request, HttpResponse.BodyHandlers.ofString());
 
	// Then
	assertEquals(HttpURLConnection.HTTP_OK, response.statusCode());}}
