/*
 * Copyright 2002-2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mytweetstats;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.view.RedirectView;
import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.auth.RequestToken;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.TimeZone;
import java.util.stream.Collectors;

@Controller
@SpringBootApplication
public class Main {

    @Value("${spring.datasource.url}")
    private String dbUrl;

    @Autowired
    private DataSource dataSource;

    public static void main(String[] args) throws Exception {
        SpringApplication.run(Main.class, args);
    }

    @RequestMapping("/")
    RedirectView login(HttpServletRequest request, HttpSession session) {

        Twitter twitter = new TwitterFactory().getInstance();
        session.setAttribute("twitter", twitter);
        RequestToken requestToken = null;

        try {
            StringBuffer callbackURL = request.getRequestURL();
            int index = callbackURL.lastIndexOf("/");
            callbackURL.replace(index, callbackURL.length(), "").append("/callback");

            requestToken = twitter.getOAuthRequestToken(callbackURL.toString());
            session.setAttribute("requestToken", requestToken);
            return new RedirectView(requestToken.getAuthenticationURL());

        } catch (TwitterException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @RequestMapping("/callback")
    RedirectView callback(HttpServletRequest request, HttpSession session) {

        Twitter twitter = (Twitter) request.getSession().getAttribute("twitter");
        RequestToken requestToken = (RequestToken) request.getSession().getAttribute("requestToken");
        String verifier = request.getParameter("oauth_verifier");
        try {
            twitter.getOAuthAccessToken(requestToken, verifier);
            request.getSession().removeAttribute("requestToken");
        } catch (TwitterException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        return new RedirectView(request.getContextPath() + "/about");

    }

    @RequestMapping("/about")
    String about(HttpServletRequest request, HttpSession session, Map<String, Object> model) {


        Twitter twitter = (Twitter) session.getAttribute("twitter");
        List<Status> statuses = getTweets(twitter);
        session.setAttribute("statuses", statuses);

        return "about";

    }


    @RequestMapping("/recent")
    String recent(HttpServletRequest request, Map<String, Object> model) {

        ArrayList<String> tweetList = new ArrayList<String>();
        Twitter twitter = (Twitter) request.getSession().getAttribute("twitter");

        // If session expired then force login again
        if (twitter == null) {
            return "redirect:/";
        }

        List<Status> statuses = (List<Status>) request.getSession().getAttribute("statuses");
        SimpleDateFormat sdf = new SimpleDateFormat("dd-MM HH:mm");
        sdf.setTimeZone(TimeZone.getTimeZone("Australia/NSW"));

        for (Status status : statuses) {
            String date = sdf.format(status.getCreatedAt());
            String screenName = status.getUser().getScreenName() + " (" + status.getUser().getName() + ")";
            tweetList.add(date + "\n@" + screenName + " - " + status.getText());
        }

        model.put("records", tweetList);
        return "show";
    }

    private List<Status> getTweets(Twitter twitter) {

        User user = null;
        try {
            user = twitter.verifyCredentials();
        } catch (TwitterException e) {
            e.printStackTrace();
        }

        System.out.println(user.getRateLimitStatus());

        List<Status> statuses = null;
        try {

            // Get last 100 tweets
            statuses = twitter.getHomeTimeline();
            statuses.addAll(twitter.getHomeTimeline(new Paging(2)));
            statuses.addAll(twitter.getHomeTimeline(new Paging(3)));
            statuses.addAll(twitter.getHomeTimeline(new Paging(4)));


        } catch (TwitterException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return statuses;
    }


    @RequestMapping("/topusers")
    String topusers(HttpServletRequest request, Map<String, Object> model) {

        Twitter twitter = (Twitter) request.getSession().getAttribute("twitter");

        // If session expired then force login again
        if (twitter == null) {
            return "redirect:/";
        }

        List<Status> statuses = (List<Status>) request.getSession().getAttribute("statuses");

        Map<String, Integer> tweetCount = new HashMap();
        SimpleDateFormat sdf = new SimpleDateFormat("dd-MM HH:mm");
        sdf.setTimeZone(TimeZone.getTimeZone("Australia/NSW"));

        for (Status status : statuses) {
            String screenName = status.getUser().getScreenName() + " (" + status.getUser().getName() + ")";
            tweetCount.put(screenName, tweetCount.getOrDefault(screenName, 0) + 1);
        }
        ArrayList<String> topTweeters = sortAndConvertMapToStringList(tweetCount);

        model.put("records", topTweeters);
        return "show";
    }

    @RequestMapping("/topclients")
    String topClients(HttpServletRequest request, Map<String, Object> model) {

        Twitter twitter = (Twitter) request.getSession().getAttribute("twitter");

        // If session expired then force login again
        if (twitter == null) {
            return "redirect:/";
        }

        List<Status> statuses = (List<Status>) request.getSession().getAttribute("statuses");

        Map<String, Integer> clientCount = new HashMap();
        Map<String, String> userClient = new HashMap();
        SimpleDateFormat sdf = new SimpleDateFormat("dd-MM HH:mm");
        sdf.setTimeZone(TimeZone.getTimeZone("Australia/NSW"));

        for (Status status : statuses) {
            String sourceUrl = status.getSource();
            String screenName = status.getUser().getScreenName() + " (" + status.getUser().getName() + ")";
            String client =  sourceUrl.substring(sourceUrl.indexOf('>') +1, sourceUrl.length()-4) ;
            userClient.put(screenName, client);
        }

        for (Map.Entry<String, String> entry : userClient.entrySet()) {
            clientCount.put(entry.getValue(), clientCount.getOrDefault(entry.getValue(), 0) + 1);
            System.out.println(entry.getKey() + " " + entry.getValue());
        }

        ArrayList<String> topClients = sortAndConvertMapToStringList(clientCount);

        model.put("records", topClients);
        return "show";
    }

    @RequestMapping("/toptweeters")
    String topTweeters(HttpServletRequest request, Map<String, Object> model) {

        Twitter twitter = (Twitter) request.getSession().getAttribute("twitter");

        // If session expired then force login again
        if (twitter == null) {
            return "redirect:/";
        }

        List<Status> statuses = (List<Status>) request.getSession().getAttribute("statuses");

        Map<String, Integer> clientCount = new HashMap();

        SimpleDateFormat sdf = new SimpleDateFormat("dd-MM HH:mm");
        sdf.setTimeZone(TimeZone.getTimeZone("Australia/NSW"));

        for (Status status : statuses) {
            String screenName = status.getUser().getScreenName() + " (" + status.getUser().getName() + ")";
            clientCount.put(screenName, status.getUser().getStatusesCount());
        }

        ArrayList<String> topClients = sortAndConvertMapToStringList(clientCount);

        model.put("records", topClients);
        return "show";
    }


    private ArrayList<String> sortAndConvertMapToStringList(Map<String, Integer> tweetCount) {
        // Sort users by number of tweets
        Map<String, Integer> sortedMap = tweetCount.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

        ArrayList<String> topTweeters = new ArrayList<String>();
        for (Map.Entry<String, Integer> entry : sortedMap.entrySet()) {
            topTweeters.add(Integer.toString(entry.getValue()) + ' ' + entry.getKey());
        }
        return topTweeters;
    }


    @RequestMapping("/db")
    String db(Map<String, Object> model) {
        try (Connection connection = dataSource.getConnection()) {
            Statement stmt = connection.createStatement();
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS ticks (tick timestamp)");
            stmt.executeUpdate("INSERT INTO ticks VALUES (now())");
            ResultSet rs = stmt.executeQuery("SELECT tick FROM ticks");

            ArrayList<String> output = new ArrayList<String>();
            while (rs.next()) {
                output.add("Read from DB: " + rs.getTimestamp("tick"));
            }

            model.put("records", output);
            return "db";
        } catch (Exception e) {
            model.put("message", e.getMessage());
            return "error";
        }
    }

    @Bean
    public DataSource dataSource() throws SQLException {
        if (dbUrl == null || dbUrl.isEmpty()) {
            return new HikariDataSource();
        } else {
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(dbUrl);
            return new HikariDataSource(config);
        }
    }

}
