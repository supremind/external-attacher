package controller

import (
	"errors"
	"fmt"

	storage "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
)

const (
	readonlyAttachmentKey = "csi.supremind.com/readonly-attach"
)

func (h *csiHandler) checkIfReadonlyMount(va *storage.VolumeAttachment) (bool, error) {
	pos, err := h.poLister.List(labels.Everything())
	if err != nil {
		return false, fmt.Errorf("list pods, %w", err)
	}

	node := va.Spec.NodeName
	claim, err := h.getClaimName(*va.Spec.Source.PersistentVolumeName)
	if err != nil {
		return false, err
	}

	for _, po := range pos {
		if po.Namespace != claim.Namespace {
			continue
		}
		if po.Spec.NodeName != node {
			continue
		}
		for _, vol := range po.Spec.Volumes {
			if vol.PersistentVolumeClaim != nil {
				if vol.PersistentVolumeClaim.ClaimName == claim.Name {
					if !vol.PersistentVolumeClaim.ReadOnly {
						return false, nil
					}
				}
			}
		}
	}

	return true, nil
}

func (h *csiHandler) checkIfROXMount(va *storage.VolumeAttachment) (bool, error) {
	vas, err := h.vaLister.List(labels.Everything())
	if err != nil {
		return false, fmt.Errorf("list volume attachments, %w", err)
	}

	node := va.Spec.NodeName

	for _, target := range vas {
		if *target.Spec.Source.PersistentVolumeName != *va.Spec.Source.PersistentVolumeName {
			continue
		}
		// exclude current va itself
		if target.Spec.NodeName == node {
			continue
		}
		if target.Status.Attached && target.Status.AttachmentMetadata[readonlyAttachmentKey] != "true" {
			return false, nil
		}
	}

	return true, nil
}

func (h *csiHandler) checkIfAttachedToOtherNodes(va *storage.VolumeAttachment) (bool, error) {
	vas, err := h.vaLister.List(labels.Everything())
	if err != nil {
		return false, fmt.Errorf("list volume attachments, %w", err)
	}

	node := va.Spec.NodeName

	for _, target := range vas {
		if *target.Spec.Source.PersistentVolumeName != *va.Spec.Source.PersistentVolumeName {
			continue
		}
		// exclude current va itself
		if target.Spec.NodeName == node {
			continue
		}
		// fixme: there could be some race condition when another attaching (r/w or ro) is in progress and has not set metadata yet.
		if target.Status.Attached {
			return true, nil
		}
	}
	return false, nil
}

func (h *csiHandler) getClaimName(pvName string) (*types.NamespacedName, error) {
	pv, err := h.pvLister.Get(pvName)
	if err != nil {
		return nil, fmt.Errorf("get persisten volume, %w", err)
	}
	if pv.Spec.ClaimRef == nil {
		return nil, errors.New("can not get claim ref for persistent volume")
	}
	claim := pv.Spec.ClaimRef
	return &types.NamespacedName{Namespace: claim.Namespace, Name: claim.Name}, nil
}
