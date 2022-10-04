package com.onthegomap.planetiler.custommap;

import com.onthegomap.planetiler.custommap.expression.ScriptEnvironment;

public class TestContexts {
  public static final Contexts.Root ROOT = Contexts.emptyRoot();
  public static final ScriptEnvironment<Contexts.Root> ROOT_CONTEXT = ROOT.description();
  public static final ScriptEnvironment<Contexts.ProcessFeature> PROCESS_FEATURE =
    Contexts.ProcessFeature.description(ROOT);
  public static final ScriptEnvironment<Contexts.FeaturePostMatch> FEATURE_POST_MATCH =
    Contexts.FeaturePostMatch.description(ROOT);
  public static final ScriptEnvironment<Contexts.FeatureAttribute> FEATURE_ATTRIBUTE =
    Contexts.FeatureAttribute.description(ROOT);
}
