package org.swm.kafkapractice.dlt;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/admin/dlt")
@RequiredArgsConstructor
public class DltReplayController {

    private final DltReplayService replayService;

    @PostMapping("/replay")
    public ResponseEntity<DltReplayResponse> replay(@RequestBody DltReplayRequest request) {
        try {
            int replayed = replayService.replay(request);
            return ResponseEntity.ok(new DltReplayResponse(replayed, null));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(new DltReplayResponse(0, e.getMessage()));
        } catch (Exception e) {
            log.error("DLT replay failed", e);
            return ResponseEntity.internalServerError().body(new DltReplayResponse(0, e.getMessage()));
        }
    }

    public record DltReplayRequest(String dltTopic, String targetTopic, int maxMessages) {}
    public record DltReplayResponse(int replayed, String error) {}
}
