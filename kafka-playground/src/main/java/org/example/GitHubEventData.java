package org.example;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Objects;

public class GitHubEventData {
    public String id;
    public String type;
    public String createdAt;
    public String action;
    public Integer payloadSize;
    public ArrayList<String> commitMessage;
    public String mainLanguage;
    public String defaultBranch;

    public GitHubEventData(String rawData) {
        JSONObject jsonData = new JSONObject(rawData);
        id = jsonData.getString("id");
        type = jsonData.getString("type");
        createdAt = jsonData.getString("created_at");
        commitMessage = new ArrayList<>();

        JSONObject payload = new JSONObject(jsonData.get("payload").toString());

        if (payload.has("commits")) {
            JSONArray commitsData = new JSONArray(payload.get("commits").toString());
            for (int i = 0; i < commitsData.length(); i++) {
                JSONObject commitData = commitsData.getJSONObject(i);
                commitMessage.addLast(commitData.getString("message"));
            }
        }

        if (payload.has("action")) {
            action = payload.getString("action");
        }

        if (payload.has("size")) {
            payloadSize = payload.getInt("size");
        }

        if (payload.has("pull_request")) {
            JSONObject head = new JSONObject(new JSONObject(payload.get("pull_request").toString()).get("head").toString());
            if (head.has("repo") && !Objects.equals(head.get("repo").toString(), "null")) {
                JSONObject repo = new JSONObject(head.get("repo").toString());
                mainLanguage = repo.get("language").toString();
                defaultBranch = repo.get("default_branch").toString();
            }
        }
    }
}
