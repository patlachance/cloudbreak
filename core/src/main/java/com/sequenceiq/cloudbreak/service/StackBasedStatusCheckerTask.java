package com.sequenceiq.cloudbreak.service;

import org.springframework.beans.factory.annotation.Autowired;

import com.sequenceiq.cloudbreak.domain.Stack;
import com.sequenceiq.cloudbreak.repository.StackRepository;

public abstract class StackBasedStatusCheckerTask<T extends StackContext> extends SimpleStatusCheckerTask<T> {

    @Autowired
    private StackRepository stackRepository;

    public boolean exitPolling(T t) {
        try {
            Stack stack = stackRepository.findById(t.getStack().getId());
            if (stack == null || stack.isDeleteInProgress()) {
                return true;
            }
            return false;
        } catch (Exception ex) {
            return true;
        }
    }

}
