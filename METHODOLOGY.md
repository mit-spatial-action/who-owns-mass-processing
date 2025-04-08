---
title: "_Who Owns Massachusetts_ Methodology"
author:
- Eric Robsky Huntley
- Anastasia Aizman
date: "2025-04-08"
output: pdf_document
lang: "en-US"
abstract: 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nullam mollis
  efficitur mi non venenatis. Donec et nisi porttitor, mattis turpis nec, hendrerit
  eros. Maecenas id sem purus. Vestibulum aliquam massa est, eget iaculis sem bibendum
  ut. Nam nibh massa, scelerisque non purus nec, feugiat pharetra ante. Mauris porta
  aliquam leo, iaculis convallis libero egestas id. Nullam semper consectetur lacus,
  vel porttitor neque tempus ut. Vestibulum non scelerisque purus. Proin velit dui,
  maximus non feugiat sed, dignissim at justo. Donec tristique orci a elementum scelerisque.
  Etiam varius auctor lacus. Duis egestas eleifend eros sit amet volutpat.'
thanks: "testingggggg"
---

## Methods

### "Dry" Parcel Geometries

### Assessors to Owners and Sites

The MassGIS assessors tables are not normalized, in the sense that a single row contains references to multiple object types. Each row represents a property; each row also contains information about its *parcel* (a straightforward many-to-one relationship), its *address*, its *owner*, and its *owner's address*. We undertake a normalization process that treats these entities as instances of their type; in other words, both addresses and owner addresses are addresses, which can have relationships to owners, parcels, and, eventually, corporate entities drawn from the OpenCorporates database.

We begin by
