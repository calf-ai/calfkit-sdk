# Changelog

## [0.1.7](https://github.com/calf-ai/calfkit-sdk/compare/v0.1.6...v0.1.7) (2026-02-20)


### Features

* agent-to-agent communication and groupchat support  ([#56](https://github.com/calf-ai/calfkit-sdk/issues/56)) ([8d7fd4d](https://github.com/calf-ai/calfkit-sdk/commit/8d7fd4d17e91d1f31a8ac70355bbbaab452f5ed6))
* declarative private topic subscriptions and caller pattern updates ([#61](https://github.com/calf-ai/calfkit-sdk/issues/61)) ([b37c423](https://github.com/calf-ai/calfkit-sdk/commit/b37c4238fc4db7d2ef993efe6945ed45e68f5668))
* private reply-topic routing for multi-agent deployments and trading bots example  ([#58](https://github.com/calf-ai/calfkit-sdk/issues/58)) ([3f13c80](https://github.com/calf-ai/calfkit-sdk/commit/3f13c807fe746a511d5f2295de5c8a067a407b92))
* stack-based agent handoff with @entrypoint/[@returnpoint](https://github.com/returnpoint) decorators ([#63](https://github.com/calf-ai/calfkit-sdk/issues/63)) ([2418fd6](https://github.com/calf-ai/calfkit-sdk/commit/2418fd6bf173f2b979c4a48654c86a0058866c6e))


### Documentation

* add support to documentation and improve readability ([#50](https://github.com/calf-ai/calfkit-sdk/issues/50)) ([e333239](https://github.com/calf-ai/calfkit-sdk/commit/e333239f28e85a6db3181c37cfbec1997b67fb18))
* calfkit cloud interest form ([#53](https://github.com/calf-ai/calfkit-sdk/issues/53)) ([5206020](https://github.com/calf-ai/calfkit-sdk/commit/5206020c44a5951fcee99770968aa60dc3a15943))
* update calfkit cloud docs and add linkedin contact ([#55](https://github.com/calf-ai/calfkit-sdk/issues/55)) ([bcf9fb5](https://github.com/calf-ai/calfkit-sdk/commit/bcf9fb5f2b6d44c7911be7139715c351f88e00d8))
* update readme ([#57](https://github.com/calf-ai/calfkit-sdk/issues/57)) ([bd22061](https://github.com/calf-ai/calfkit-sdk/commit/bd2206197113db7f5753b36b6f49614df72e3276))
* use TIP syntax for Calfkit Cloud callout ([#54](https://github.com/calf-ai/calfkit-sdk/issues/54)) ([5c6fa4e](https://github.com/calf-ai/calfkit-sdk/commit/5c6fa4e6b3a0e6cc6bbf05df74ef3d03cfadc27f))

## [0.1.6](https://github.com/calf-ai/calfkit-sdk/compare/v0.1.5...v0.1.6) (2026-02-06)

### Refactor
* vendor pydantic-ai-slim to reduce package install footprint by 81% ([#48](https://github.com/calf-ai/calfkit-sdk/issues/48)) ([a31ca78](https://github.com/calf-ai/calfkit-sdk/commit/a31ca78))

### Documentation

* add python ver. compatibility and add shareable tool explanation ([#46](https://github.com/calf-ai/calfkit-sdk/issues/46)) ([a22e23b](https://github.com/calf-ai/calfkit-sdk/commit/a22e23b22024ba34b8ff72d286d91ef516a9a9e5))
* update README.md for readability ([#49](https://github.com/calf-ai/calfkit-sdk/issues/49)) ([b4c4f78](https://github.com/calf-ai/calfkit-sdk/commit/b4c4f78e7e979bed2145ff1c0522986fa7d4bd85))

## [0.1.5](https://github.com/calf-ai/calfkit-sdk/compare/v0.1.4...v0.1.5) (2026-02-06)


### Bug Fixes

* distinguish between unset and empty `tool_nodes` in runtime patching + docs: dynamic runtime tools and system prompt patching ([#44](https://github.com/calf-ai/calfkit-sdk/issues/44)) ([7805e1a](https://github.com/calf-ai/calfkit-sdk/commit/7805e1ab522fec2e061138701f2a85ebd7e73689))


### Documentation

* add contact section ([#43](https://github.com/calf-ai/calfkit-sdk/issues/43)) ([a9ec018](https://github.com/calf-ai/calfkit-sdk/commit/a9ec018b12bc155e374e466215cf1b1c3867bdba))
* fix quickstart documentation for tool patching to be more clear ([#45](https://github.com/calf-ai/calfkit-sdk/issues/45)) ([dbad7af](https://github.com/calf-ai/calfkit-sdk/commit/dbad7affcda7c8477d61971345a67b166f491833))
* update README.md wording ([#39](https://github.com/calf-ai/calfkit-sdk/issues/39)) ([c8fe616](https://github.com/calf-ai/calfkit-sdk/commit/c8fe6168d5f6abe4745daafe3b52a877dd12bdc8))
* update README.md wording ([#41](https://github.com/calf-ai/calfkit-sdk/issues/41)) ([191836c](https://github.com/calf-ai/calfkit-sdk/commit/191836c38251fecbd3f87a388bd84821cedf6166))
* update README.md wording ([#42](https://github.com/calf-ai/calfkit-sdk/issues/42)) ([bd43c32](https://github.com/calf-ai/calfkit-sdk/commit/bd43c3233feb0838a1133c1f7dbd9524cb19f4b1))

## [0.1.4](https://github.com/calf-ai/calfkit-sdk/compare/v0.1.3...v0.1.4) (2026-02-05)


### Documentation

* improve README with quickstart refinements and badges ([#37](https://github.com/calf-ai/calfkit-sdk/issues/37)) ([19e089e](https://github.com/calf-ai/calfkit-sdk/commit/19e089e54f841980c4ccdcff65e9e74579d8db74))

## [0.1.3](https://github.com/calf-ai/calfkit-sdk/compare/v0.1.2...v0.1.3) (2026-02-05)


### Features

* add RouterServiceClient for invoking agents services ([#36](https://github.com/calf-ai/calfkit-sdk/issues/36)) ([8cd6eca](https://github.com/calf-ai/calfkit-sdk/commit/8cd6ecaec18c4a34e255a2592b097ec69cb53318))
* new `Service` class to deploy and run nodes as a service ([#33](https://github.com/calf-ai/calfkit-sdk/issues/33)) ([7066bf3](https://github.com/calf-ai/calfkit-sdk/commit/7066bf3329a9dd91a3e4abb8ee1d91a2001f1d53))

## [0.1.2](https://github.com/calf-ai/calfkit-sdk/compare/v0.1.1...v0.1.2) (2026-02-04)


### Bug Fixes

* invoke function on unstarted broker + doc: readme quickstart examples ([#30](https://github.com/calf-ai/calfkit-sdk/issues/30)) ([bad4216](https://github.com/calf-ai/calfkit-sdk/commit/bad4216a779fe785f76bc627d4c61a98df800922))

## [0.1.1](https://github.com/calf-ai/calf-sdk/compare/v0.1.0...v0.1.1) (2026-02-03)


### Features

* add build commands and CI workflows ([#3](https://github.com/calf-ai/calf-sdk/issues/3)) ([fa66282](https://github.com/calf-ai/calf-sdk/commit/fa66282d31c139d6b6579e6e13c54ecf02ab759d))
* event-driven tool calling + agent routing node + function tool decorator ([#16](https://github.com/calf-ai/calf-sdk/issues/16)) ([5af52bd](https://github.com/calf-ai/calf-sdk/commit/5af52bd779c0b567cf80395063b622ef9d9cc388))
* high level api structure skeleton ([#8](https://github.com/calf-ai/calf-sdk/issues/8)) ([960b622](https://github.com/calf-ai/calf-sdk/commit/960b622a4966d60f018b73a4e140015c24a2f1bd))
* implement chat agent node + refactor provider layer ([#12](https://github.com/calf-ai/calf-sdk/issues/12)) ([c2e40c0](https://github.com/calf-ai/calf-sdk/commit/c2e40c0a8e0417f8d3097a786c09ed45144bc0f9))
* implement event-driven base node pieces and refactor model clients ([#10](https://github.com/calf-ai/calf-sdk/issues/10)) ([bb66af0](https://github.com/calf-ai/calf-sdk/commit/bb66af0a17380e4e892e31442ff68d3e50e4fd8f))
* implement node-based agent architecture with event-driven messaging ([#15](https://github.com/calf-ai/calf-sdk/issues/15)) ([0bfd190](https://github.com/calf-ai/calf-sdk/commit/0bfd190a60f1c5b9dbf9815c100014078106b27b))
* message history persistence ([#18](https://github.com/calf-ai/calf-sdk/issues/18)) ([ef6dfae](https://github.com/calf-ai/calf-sdk/commit/ef6dfaee10eff29e71af64bf263dd275f488a78b))
* MVP agents ([#17](https://github.com/calf-ai/calf-sdk/issues/17)) ([f6141d4](https://github.com/calf-ai/calf-sdk/commit/f6141d46d0f93ff13173cc2249bedd3fd735ecab))
* openai chat completions client and base client class implementation ([#9](https://github.com/calf-ai/calf-sdk/issues/9)) ([28f6d62](https://github.com/calf-ai/calf-sdk/commit/28f6d62e72ff287b4ee7095992c0da2c07ff5d7c))
* rename sdk to calfkit ([#26](https://github.com/calf-ai/calf-sdk/issues/26)) ([32d114f](https://github.com/calf-ai/calf-sdk/commit/32d114f14d537da279ebd3975d7df0511648ccfa))


### Bug Fixes

* mypy ci issues ([#23](https://github.com/calf-ai/calf-sdk/issues/23)) ([4429541](https://github.com/calf-ai/calf-sdk/commit/442954193fd6a64517f5cecef005018efec13622))
* refine sdk and bug fixes ([#24](https://github.com/calf-ai/calf-sdk/issues/24)) ([89a586b](https://github.com/calf-ai/calf-sdk/commit/89a586bd40bfa5d32659a5e97c9f70fd3df38062))


### Dependencies

* update aiokafka requirement from &lt;0.13.0,&gt;=0.10.0 to &gt;=0.10.0,&lt;0.14.0 ([#13](https://github.com/calf-ai/calf-sdk/issues/13)) ([13b5014](https://github.com/calf-ai/calf-sdk/commit/13b501411a569a44a9baadccad821174c91deac7))
* update typer requirement from &lt;0.20 to &lt;0.22 ([#14](https://github.com/calf-ai/calf-sdk/issues/14)) ([c7ef948](https://github.com/calf-ai/calf-sdk/commit/c7ef9483d75b5cc2cbcfb57dc32e0a01b15b82ab))


### Documentation

* high-level agents, teams, tools API documentation and example code usage snippets ([#6](https://github.com/calf-ai/calf-sdk/issues/6)) ([de81b63](https://github.com/calf-ai/calf-sdk/commit/de81b6353c4659f47302f8f37c78607ce6154f44))
* update readme consistency ([#7](https://github.com/calf-ai/calf-sdk/issues/7)) ([279ccd1](https://github.com/calf-ai/calf-sdk/commit/279ccd1f5f2b6632ca27fb527e781a588205a419))
