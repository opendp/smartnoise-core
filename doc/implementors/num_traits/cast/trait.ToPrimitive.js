(function() {var implementors = {};
implementors["noisy_float"] = [{"text":"impl&lt;F:&nbsp;Float, C:&nbsp;FloatChecker&lt;F&gt;&gt; ToPrimitive for NoisyFloat&lt;F, C&gt;","synthetic":false,"types":[]}];
implementors["num_bigint"] = [{"text":"impl ToPrimitive for BigInt","synthetic":false,"types":[]},{"text":"impl ToPrimitive for BigUint","synthetic":false,"types":[]}];
implementors["num_complex"] = [{"text":"impl&lt;T:&nbsp;ToPrimitive + Num&gt; ToPrimitive for Complex&lt;T&gt;","synthetic":false,"types":[]}];
implementors["num_rational"] = [{"text":"impl&lt;T:&nbsp;Clone + Integer + ToPrimitive + ToBigInt&gt; ToPrimitive for Ratio&lt;T&gt;","synthetic":false,"types":[]}];
implementors["num_traits"] = [];
if (window.register_implementors) {window.register_implementors(implementors);} else {window.pending_implementors = implementors;}})()