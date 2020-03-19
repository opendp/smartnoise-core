(function() {var implementors = {};
implementors["ndarray"] = [{"text":"impl&lt;'a, A, S, S2, D, E&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/bit/trait.ShrAssign.html\" title=\"trait core::ops::bit::ShrAssign\">ShrAssign</a>&lt;&amp;'a <a class=\"struct\" href=\"ndarray/struct.ArrayBase.html\" title=\"struct ndarray::ArrayBase\">ArrayBase</a>&lt;S2, E&gt;&gt; for <a class=\"struct\" href=\"ndarray/struct.ArrayBase.html\" title=\"struct ndarray::ArrayBase\">ArrayBase</a>&lt;S, D&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;A: <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/clone/trait.Clone.html\" title=\"trait core::clone::Clone\">Clone</a> + <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/bit/trait.ShrAssign.html\" title=\"trait core::ops::bit::ShrAssign\">ShrAssign</a>&lt;A&gt;,<br>&nbsp;&nbsp;&nbsp;&nbsp;S: <a class=\"trait\" href=\"ndarray/trait.DataMut.html\" title=\"trait ndarray::DataMut\">DataMut</a>&lt;Elem = A&gt;,<br>&nbsp;&nbsp;&nbsp;&nbsp;S2: <a class=\"trait\" href=\"ndarray/trait.Data.html\" title=\"trait ndarray::Data\">Data</a>&lt;Elem = A&gt;,<br>&nbsp;&nbsp;&nbsp;&nbsp;D: <a class=\"trait\" href=\"ndarray/trait.Dimension.html\" title=\"trait ndarray::Dimension\">Dimension</a>,<br>&nbsp;&nbsp;&nbsp;&nbsp;E: <a class=\"trait\" href=\"ndarray/trait.Dimension.html\" title=\"trait ndarray::Dimension\">Dimension</a>,&nbsp;</span>","synthetic":false,"types":["ndarray::ArrayBase"]},{"text":"impl&lt;A, S, D&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/bit/trait.ShrAssign.html\" title=\"trait core::ops::bit::ShrAssign\">ShrAssign</a>&lt;A&gt; for <a class=\"struct\" href=\"ndarray/struct.ArrayBase.html\" title=\"struct ndarray::ArrayBase\">ArrayBase</a>&lt;S, D&gt; <span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;A: <a class=\"trait\" href=\"ndarray/trait.ScalarOperand.html\" title=\"trait ndarray::ScalarOperand\">ScalarOperand</a> + <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/bit/trait.ShrAssign.html\" title=\"trait core::ops::bit::ShrAssign\">ShrAssign</a>&lt;A&gt;,<br>&nbsp;&nbsp;&nbsp;&nbsp;S: <a class=\"trait\" href=\"ndarray/trait.DataMut.html\" title=\"trait ndarray::DataMut\">DataMut</a>&lt;Elem = A&gt;,<br>&nbsp;&nbsp;&nbsp;&nbsp;D: <a class=\"trait\" href=\"ndarray/trait.Dimension.html\" title=\"trait ndarray::Dimension\">Dimension</a>,&nbsp;</span>","synthetic":false,"types":["ndarray::ArrayBase"]}];
implementors["num_bigint"] = [{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/bit/trait.ShrAssign.html\" title=\"trait core::ops::bit::ShrAssign\">ShrAssign</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.usize.html\">usize</a>&gt; for <a class=\"struct\" href=\"num_bigint/struct.BigInt.html\" title=\"struct num_bigint::BigInt\">BigInt</a>","synthetic":false,"types":["num_bigint::bigint::BigInt"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/bit/trait.ShrAssign.html\" title=\"trait core::ops::bit::ShrAssign\">ShrAssign</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.usize.html\">usize</a>&gt; for <a class=\"struct\" href=\"num_bigint/struct.BigUint.html\" title=\"struct num_bigint::BigUint\">BigUint</a>","synthetic":false,"types":["num_bigint::biguint::BigUint"]}];
implementors["rug"] = [{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/bit/trait.ShrAssign.html\" title=\"trait core::ops::bit::ShrAssign\">ShrAssign</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.i32.html\">i32</a>&gt; for <a class=\"struct\" href=\"rug/struct.Integer.html\" title=\"struct rug::Integer\">Integer</a>","synthetic":false,"types":["rug::integer::big::Integer"]},{"text":"impl&lt;'_&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/bit/trait.ShrAssign.html\" title=\"trait core::ops::bit::ShrAssign\">ShrAssign</a>&lt;&amp;'_ <a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.i32.html\">i32</a>&gt; for <a class=\"struct\" href=\"rug/struct.Integer.html\" title=\"struct rug::Integer\">Integer</a>","synthetic":false,"types":["rug::integer::big::Integer"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/bit/trait.ShrAssign.html\" title=\"trait core::ops::bit::ShrAssign\">ShrAssign</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.u32.html\">u32</a>&gt; for <a class=\"struct\" href=\"rug/struct.Integer.html\" title=\"struct rug::Integer\">Integer</a>","synthetic":false,"types":["rug::integer::big::Integer"]},{"text":"impl&lt;'_&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/bit/trait.ShrAssign.html\" title=\"trait core::ops::bit::ShrAssign\">ShrAssign</a>&lt;&amp;'_ <a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.u32.html\">u32</a>&gt; for <a class=\"struct\" href=\"rug/struct.Integer.html\" title=\"struct rug::Integer\">Integer</a>","synthetic":false,"types":["rug::integer::big::Integer"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/bit/trait.ShrAssign.html\" title=\"trait core::ops::bit::ShrAssign\">ShrAssign</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.i32.html\">i32</a>&gt; for <a class=\"struct\" href=\"rug/struct.Rational.html\" title=\"struct rug::Rational\">Rational</a>","synthetic":false,"types":["rug::rational::big::Rational"]},{"text":"impl&lt;'_&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/bit/trait.ShrAssign.html\" title=\"trait core::ops::bit::ShrAssign\">ShrAssign</a>&lt;&amp;'_ <a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.i32.html\">i32</a>&gt; for <a class=\"struct\" href=\"rug/struct.Rational.html\" title=\"struct rug::Rational\">Rational</a>","synthetic":false,"types":["rug::rational::big::Rational"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/bit/trait.ShrAssign.html\" title=\"trait core::ops::bit::ShrAssign\">ShrAssign</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.u32.html\">u32</a>&gt; for <a class=\"struct\" href=\"rug/struct.Rational.html\" title=\"struct rug::Rational\">Rational</a>","synthetic":false,"types":["rug::rational::big::Rational"]},{"text":"impl&lt;'_&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/bit/trait.ShrAssign.html\" title=\"trait core::ops::bit::ShrAssign\">ShrAssign</a>&lt;&amp;'_ <a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.u32.html\">u32</a>&gt; for <a class=\"struct\" href=\"rug/struct.Rational.html\" title=\"struct rug::Rational\">Rational</a>","synthetic":false,"types":["rug::rational::big::Rational"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/bit/trait.ShrAssign.html\" title=\"trait core::ops::bit::ShrAssign\">ShrAssign</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.u32.html\">u32</a>&gt; for <a class=\"struct\" href=\"rug/struct.Float.html\" title=\"struct rug::Float\">Float</a>","synthetic":false,"types":["rug::float::big::Float"]},{"text":"impl&lt;'_&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/bit/trait.ShrAssign.html\" title=\"trait core::ops::bit::ShrAssign\">ShrAssign</a>&lt;&amp;'_ <a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.u32.html\">u32</a>&gt; for <a class=\"struct\" href=\"rug/struct.Float.html\" title=\"struct rug::Float\">Float</a>","synthetic":false,"types":["rug::float::big::Float"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/bit/trait.ShrAssign.html\" title=\"trait core::ops::bit::ShrAssign\">ShrAssign</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.i32.html\">i32</a>&gt; for <a class=\"struct\" href=\"rug/struct.Float.html\" title=\"struct rug::Float\">Float</a>","synthetic":false,"types":["rug::float::big::Float"]},{"text":"impl&lt;'_&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/bit/trait.ShrAssign.html\" title=\"trait core::ops::bit::ShrAssign\">ShrAssign</a>&lt;&amp;'_ <a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.i32.html\">i32</a>&gt; for <a class=\"struct\" href=\"rug/struct.Float.html\" title=\"struct rug::Float\">Float</a>","synthetic":false,"types":["rug::float::big::Float"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/bit/trait.ShrAssign.html\" title=\"trait core::ops::bit::ShrAssign\">ShrAssign</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.u32.html\">u32</a>&gt; for <a class=\"struct\" href=\"rug/struct.Complex.html\" title=\"struct rug::Complex\">Complex</a>","synthetic":false,"types":["rug::complex::big::Complex"]},{"text":"impl&lt;'_&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/bit/trait.ShrAssign.html\" title=\"trait core::ops::bit::ShrAssign\">ShrAssign</a>&lt;&amp;'_ <a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.u32.html\">u32</a>&gt; for <a class=\"struct\" href=\"rug/struct.Complex.html\" title=\"struct rug::Complex\">Complex</a>","synthetic":false,"types":["rug::complex::big::Complex"]},{"text":"impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/bit/trait.ShrAssign.html\" title=\"trait core::ops::bit::ShrAssign\">ShrAssign</a>&lt;<a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.i32.html\">i32</a>&gt; for <a class=\"struct\" href=\"rug/struct.Complex.html\" title=\"struct rug::Complex\">Complex</a>","synthetic":false,"types":["rug::complex::big::Complex"]},{"text":"impl&lt;'_&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/ops/bit/trait.ShrAssign.html\" title=\"trait core::ops::bit::ShrAssign\">ShrAssign</a>&lt;&amp;'_ <a class=\"primitive\" href=\"https://doc.rust-lang.org/nightly/std/primitive.i32.html\">i32</a>&gt; for <a class=\"struct\" href=\"rug/struct.Complex.html\" title=\"struct rug::Complex\">Complex</a>","synthetic":false,"types":["rug::complex::big::Complex"]}];

            if (window.register_implementors) {
                window.register_implementors(implementors);
            } else {
                window.pending_implementors = implementors;
            }
        })()