package com.sequenceiq.provisioning.controller;

import java.util.List;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.sequenceiq.provisioning.controller.json.BlueprintJson;
import com.sequenceiq.provisioning.domain.User;
import com.sequenceiq.provisioning.security.CurrentUser;
import com.sequenceiq.provisioning.service.AmbariBlueprintService;

@Controller
@RequestMapping("/stack/{stackId}/blueprint")
public class BlueprintController {

    @Autowired
    private AmbariBlueprintService ambariBlueprintService;

    @RequestMapping(method = RequestMethod.POST)
    @ResponseBody
    public ResponseEntity<String> addBlueprint(@CurrentUser User user, @PathVariable Long stackId, @RequestBody @Valid BlueprintJson blueprintRequest) {
        ambariBlueprintService.addBlueprint(user, stackId, blueprintRequest);
        return new ResponseEntity<>(HttpStatus.CREATED);
    }

    @RequestMapping(method = RequestMethod.GET)
    @ResponseBody
    public ResponseEntity<List<BlueprintJson>> retrieveBlueprints(@CurrentUser User user, @PathVariable Long stackId) {
        return new ResponseEntity<>(ambariBlueprintService.retrieveBlueprints(user, stackId), HttpStatus.OK);
    }

    @RequestMapping(method = RequestMethod.GET, value = "{id}")
    @ResponseBody
    public ResponseEntity<BlueprintJson> retrieveBlueprint(@CurrentUser User user, @PathVariable Long stackId, @PathVariable String id) {
        return new ResponseEntity<>(ambariBlueprintService.retrieveBlueprint(user, stackId, id), HttpStatus.OK);
    }
}