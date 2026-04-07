import { useEffect, useRef } from "react";

export default function AuthCanvasBackground() {
    const canvasRef = useRef(null);
    const wrapRef = useRef(null);

    useEffect(() => {
        const cv = canvasRef.current;
        if (!cv) return;

        const ctx = cv.getContext("2d");
        if (!ctx) return;

        let W;
        let H;
        let S;
        let CX;
        let CY;
        const PI2 = Math.PI * 2;
        const EPS = 0.001;
        let rafId = 0;

        const inPaths = [
            { p0: { x: -340, y: -135 }, p1: { x: -185, y: -160 }, p2: { x: -55, y: -115 }, p3: { x: 0, y: -70 } },
            { p0: { x: -365, y: 8 }, p1: { x: -210, y: -35 }, p2: { x: -75, y: -58 }, p3: { x: 0, y: -70 } },
            { p0: { x: -340, y: 150 }, p1: { x: -185, y: 85 }, p2: { x: -55, y: 12 }, p3: { x: 0, y: -70 } },
            { p0: { x: -195, y: -185 }, p1: { x: -155, y: -125 }, p2: { x: -42, y: -95 }, p3: { x: 0, y: -70 } },
        ];

        const outPaths = [
            { p0: { x: 0, y: 8 }, p1: { x: 72, y: -55 }, p2: { x: 195, y: -82 }, p3: { x: 335, y: -108 } },
            { p0: { x: 0, y: 14 }, p1: { x: 88, y: 4 }, p2: { x: 215, y: 0 }, p3: { x: 365, y: 4 } },
            { p0: { x: 0, y: 8 }, p1: { x: 72, y: 62 }, p2: { x: 195, y: 92 }, p3: { x: 335, y: 112 } },
        ];

        const docs = [];
        const cards = [];
        const particles = [];
        const bgDots = [];

        const cardTypes = ["bar", "line", "pie", "report", "bar", "line", "pie"];

        inPaths.forEach((p, pi) => {
            const n = pi < 2 ? 3 : 2;
            for (let i = 0; i < n; i += 1) {
                docs.push({
                    path: p,
                    ph: i / n,
                    spd: 0.115 + Math.random() * 0.025,
                    type: pi === 3 ? "folder" : "doc",
                    rot: (Math.random() - 0.5) * 0.12,
                    sc: 0.82 + Math.random() * 0.22,
                });
            }
        });

        outPaths.forEach((p, pi) => {
            for (let i = 0; i < 2; i += 1) {
                cards.push({
                    path: p,
                    ph: i / 2,
                    spd: 0.105 + Math.random() * 0.02,
                    type: cardTypes[pi * 2 + i] || "bar",
                    rot: (Math.random() - 0.5) * 0.06,
                    sc: 0.82 + Math.random() * 0.2,
                });
            }
        });

        inPaths.forEach((p) => {
            for (let i = 0; i < 10; i += 1) {
                particles.push({ path: p, ph: i / 10, spd: 0.14 + Math.random() * 0.06, sz: 1.2 + Math.random() * 2, t: "in" });
            }
        });

        outPaths.forEach((p) => {
            for (let i = 0; i < 10; i += 1) {
                particles.push({ path: p, ph: i / 10, spd: 0.13 + Math.random() * 0.05, sz: 1.2 + Math.random() * 2, t: "out" });
            }
        });

        for (let i = 0; i < 18; i += 1) {
            particles.push({ t: "int", ph: Math.random(), spd: 0.09 + Math.random() * 0.08, xOff: (Math.random() - 0.5) * 48, sz: 1 + Math.random() * 1.5 });
        }

        for (let i = 0; i < 30; i += 1) {
            bgDots.push({
                x: Math.random() * 1600 - 800,
                y: Math.random() * 900 - 450,
                r: 1.5 + Math.random() * 3.5,
                shape: Math.random() > 0.6 ? "sq" : "ci",
                ph: Math.random() * PI2,
                spd: 0.08 + Math.random() * 0.14,
                drift: 3 + Math.random() * 7,
                a: 0.025 + Math.random() * 0.035,
            });
        }

        const gears = [
            { x: -20, y: -6, oR: 23, iR: 16, teeth: 8, spd: 0.28, col: "#c0cef0" },
            { x: 22, y: 10, oR: 17, iR: 11, teeth: 6, spd: -0.38, col: "#ccd8f4" },
            { x: -6, y: 22, oR: 13, iR: 8, teeth: 6, spd: 0.48, col: "#d4def6" },
        ];

        function clamp(v, a, b) {
            return Math.max(a, Math.min(b, v));
        }

        function easeIO(t) {
            return t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2;
        }

        function resize() {
            const wrap = wrapRef.current;
            const width = wrap?.clientWidth || window.innerWidth;
            const height = wrap?.clientHeight || window.innerHeight;
            const dpr = Math.min(window.devicePixelRatio || 1, 2);
            W = width;
            H = height;
            cv.width = Math.floor(W * dpr);
            cv.height = Math.floor(H * dpr);
            cv.style.width = `${W}px`;
            cv.style.height = `${H}px`;
            ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
            S = Math.min(W / 1600, H / 900) * 1.42;
            CX = W * 0.52;
            CY = H * 0.56;
        }

        function rr(x, y, w, h, r) {
            const rad = Math.max(EPS, r);
            ctx.beginPath();
            ctx.moveTo(x + rad, y);
            ctx.lineTo(x + w - rad, y);
            ctx.quadraticCurveTo(x + w, y, x + w, y + rad);
            ctx.lineTo(x + w, y + h - rad);
            ctx.quadraticCurveTo(x + w, y + h, x + w - rad, y + h);
            ctx.lineTo(x + rad, y + h);
            ctx.quadraticCurveTo(x, y + h, x, y + h - rad);
            ctx.lineTo(x, y + rad);
            ctx.quadraticCurveTo(x, y, x + rad, y);
            ctx.closePath();
        }

        function bz(t, a, b, c, d) {
            const u = 1 - t;
            return {
                x: u * u * u * a.x + 3 * u * u * t * b.x + 3 * u * t * t * c.x + t * t * t * d.x,
                y: u * u * u * a.y + 3 * u * u * t * b.y + 3 * u * t * t * c.y + t * t * t * d.y,
            };
        }

        function drawBg() {
            const g = ctx.createLinearGradient(0, 0, W, H);
            g.addColorStop(0, "#eef2ff");
            g.addColorStop(0.35, "#f2f0ff");
            g.addColorStop(0.65, "#edf5ff");
            g.addColorStop(1, "#f0edff");
            ctx.fillStyle = g;
            ctx.fillRect(0, 0, W, H);

            const rg = ctx.createRadialGradient(CX, CY, 0, CX, CY, 280 * S);
            rg.addColorStop(0, "rgba(91,141,239,0.055)");
            rg.addColorStop(0.6, "rgba(155,143,239,0.025)");
            rg.addColorStop(1, "rgba(0,0,0,0)");
            ctx.fillStyle = rg;
            ctx.fillRect(0, 0, W, H);
        }

        function drawBgDots(t) {
            bgDots.forEach((d) => {
                const dx = Math.sin(t * d.spd + d.ph) * d.drift;
                const dy = Math.cos(t * d.spd * 0.7 + d.ph) * d.drift;
                const x = CX + (d.x + dx) * S;
                const y = CY + (d.y + dy) * S;
                const r = Math.max(EPS, d.r * S);

                ctx.globalAlpha = d.a;
                if (d.shape === "ci") {
                    ctx.beginPath();
                    ctx.arc(x, y, r, 0, PI2);
                    ctx.fillStyle = "#b4c2e0";
                    ctx.fill();
                } else {
                    ctx.save();
                    ctx.translate(x, y);
                    ctx.rotate(t * 0.08 + d.ph);
                    rr(-r, -r, r * 2, r * 2, 1);
                    ctx.fillStyle = "#c0ccf0";
                    ctx.fill();
                    ctx.restore();
                }
            });
            ctx.globalAlpha = 1;
        }

        function tracePath(path) {
            ctx.beginPath();
            for (let i = 0; i <= 48; i += 1) {
                const p = bz(i / 48, path.p0, path.p1, path.p2, path.p3);
                if (i === 0) ctx.moveTo(CX + p.x * S, CY + p.y * S);
                else ctx.lineTo(CX + p.x * S, CY + p.y * S);
            }
        }

        function drawPaths(t) {
            ctx.save();
            ctx.setLineDash([5 * S, 7 * S]);
            ctx.lineWidth = 1 * S;
            ctx.lineCap = "round";
            inPaths.forEach((p) => {
                ctx.strokeStyle = "rgba(91,141,239,0.09)";
                tracePath(p);
                ctx.stroke();
            });
            outPaths.forEach((p) => {
                ctx.strokeStyle = "rgba(155,143,239,0.09)";
                tracePath(p);
                ctx.stroke();
            });
            ctx.setLineDash([]);
            ctx.restore();

            ctx.save();
            ctx.lineCap = "round";
            ctx.lineWidth = 1.8 * S;
            inPaths.forEach((p) => {
                ctx.setLineDash([3 * S, 14 * S]);
                ctx.lineDashOffset = -t * 42;
                ctx.strokeStyle = "rgba(91,141,239,0.22)";
                tracePath(p);
                ctx.stroke();
            });
            outPaths.forEach((p) => {
                ctx.setLineDash([3 * S, 14 * S]);
                ctx.lineDashOffset = -t * 36;
                ctx.strokeStyle = "rgba(155,143,239,0.22)";
                tracePath(p);
                ctx.stroke();
            });
            ctx.setLineDash([]);
            ctx.restore();

            function arrow(path, col) {
                const a = bz(0.93, path.p0, path.p1, path.p2, path.p3);
                const b = bz(1, path.p0, path.p1, path.p2, path.p3);
                const ang = Math.atan2(b.y - a.y, b.x - a.x);
                const hl = 9 * S;
                ctx.fillStyle = col;
                ctx.beginPath();
                ctx.moveTo(CX + b.x * S, CY + b.y * S);
                ctx.lineTo(CX + b.x * S - hl * Math.cos(ang - 0.38), CY + b.y * S - hl * Math.sin(ang - 0.38));
                ctx.lineTo(CX + b.x * S - hl * Math.cos(ang + 0.38), CY + b.y * S - hl * Math.sin(ang + 0.38));
                ctx.closePath();
                ctx.fill();
            }

            inPaths.forEach((p) => arrow(p, "rgba(91,141,239,0.28)"));
            outPaths.forEach((p) => arrow(p, "rgba(155,143,239,0.28)"));
        }

        function drawParticles(t) {
            particles.forEach((p) => {
                const phase = (p.ph + t * p.spd) % 1;
                const op = Math.sin(phase * Math.PI);
                if (op < 0.01) return;

                let x;
                let y;
                if (p.t === "int") {
                    x = CX + p.xOff * S;
                    y = CY - 48 * S + phase * 96 * S;
                } else {
                    const pt = bz(phase, p.path.p0, p.path.p1, p.path.p2, p.path.p3);
                    x = CX + pt.x * S;
                    y = CY + pt.y * S;
                }

                const r = Math.max(EPS, p.sz * S);
                const c = p.t === "in" ? "91,141,239" : p.t === "out" ? "155,143,239" : "91,141,239";
                const g = ctx.createRadialGradient(x, y, 0, x, y, r * 3.5);
                g.addColorStop(0, `rgba(${c},${0.35 * op})`);
                g.addColorStop(1, `rgba(${c},0)`);
                ctx.fillStyle = g;
                ctx.beginPath();
                ctx.arc(x, y, r * 3.5, 0, PI2);
                ctx.fill();

                ctx.fillStyle = `rgba(${c},${0.6 * op})`;
                ctx.beginPath();
                ctx.arc(x, y, r * 0.6, 0, PI2);
                ctx.fill();
            });
        }

        function drawGear(cx, cy, oR, iR, teeth, rot, col) {
            const o = Math.max(EPS, oR);
            const i = Math.max(EPS, iR);
            ctx.save();
            ctx.translate(cx, cy);
            ctx.rotate(rot);
            ctx.globalAlpha = 0.45;
            ctx.beginPath();
            for (let n = 0; n < teeth; n += 1) {
                const a1 = (n / teeth) * PI2;
                const a2 = ((n + 0.25) / teeth) * PI2;
                const a3 = ((n + 0.5) / teeth) * PI2;
                const a4 = ((n + 0.75) / teeth) * PI2;
                if (n === 0) ctx.moveTo(Math.cos(a1) * i, Math.sin(a1) * i);
                ctx.lineTo(Math.cos(a2) * o, Math.sin(a2) * o);
                ctx.lineTo(Math.cos(a3) * o, Math.sin(a3) * o);
                ctx.lineTo(Math.cos(a4) * i, Math.sin(a4) * i);
            }
            ctx.closePath();
            ctx.fillStyle = col;
            ctx.fill();

            ctx.globalAlpha = 0.6;
            ctx.strokeStyle = "#a0b0d0";
            ctx.lineWidth = 0.8 * S;
            ctx.stroke();

            ctx.globalAlpha = 0.55;
            ctx.beginPath();
            ctx.arc(0, 0, i * 0.32, 0, PI2);
            ctx.fillStyle = "#ffffff";
            ctx.fill();
            ctx.globalAlpha = 1;
            ctx.restore();
        }

        function drawDatabase(t) {
            const rx = 78 * S;
            const ry = 23 * S;
            const h = 115 * S;

            const og = ctx.createRadialGradient(CX, CY, rx * 0.4, CX, CY, rx * 2.2);
            og.addColorStop(0, "rgba(91,141,239,0.07)");
            og.addColorStop(1, "rgba(91,141,239,0)");
            ctx.fillStyle = og;
            ctx.beginPath();
            ctx.ellipse(CX, CY, rx * 2.2, rx * 1.3, 0, 0, PI2);
            ctx.fill();

            ctx.save();
            ctx.shadowColor = "rgba(80,110,180,0.14)";
            ctx.shadowBlur = 32 * S;
            ctx.shadowOffsetY = 12 * S;
            ctx.beginPath();
            ctx.ellipse(CX, CY + h / 2, rx, ry, 0, 0, Math.PI);
            ctx.lineTo(CX - rx, CY - h / 2);
            ctx.ellipse(CX, CY - h / 2, rx, ry, 0, Math.PI, 0, true);
            ctx.closePath();

            const bg = ctx.createLinearGradient(CX - rx, CY, CX + rx, CY);
            bg.addColorStop(0, "#d2daf0");
            bg.addColorStop(0.18, "#ecf0ff");
            bg.addColorStop(0.42, "#ffffff");
            bg.addColorStop(0.68, "#f4f7ff");
            bg.addColorStop(1, "#c8d0ea");
            ctx.fillStyle = bg;
            ctx.fill();
            ctx.strokeStyle = "#aec0dc";
            ctx.lineWidth = 1.5 * S;
            ctx.stroke();
            ctx.restore();

            ctx.beginPath();
            ctx.ellipse(CX, CY + h / 2, rx, ry, 0, 0, Math.PI);
            ctx.strokeStyle = "#aec0dc";
            ctx.lineWidth = 1.5 * S;
            ctx.stroke();

            ctx.save();
            ctx.beginPath();
            ctx.ellipse(CX, CY + h / 2 - 1 * S, rx - 2 * S, ry - 1 * S, 0, 0, Math.PI);
            ctx.lineTo(CX - rx + 2 * S, CY - h / 2 + 3 * S);
            ctx.ellipse(CX, CY - h / 2 + 3 * S, rx - 2 * S, ry - 1 * S, 0, Math.PI, 0, true);
            ctx.closePath();
            ctx.clip();

            for (let i = 0; i < 5; i += 1) {
                const px = CX + (i - 2) * 24 * S;
                ctx.beginPath();
                ctx.setLineDash([4 * S, 7 * S]);
                ctx.lineDashOffset = -t * (28 + i * 5);
                ctx.strokeStyle = `rgba(91,141,239,${0.07 + i * 0.015})`;
                ctx.lineWidth = 1.5 * S;
                ctx.moveTo(px, CY - h / 2 - 4 * S);
                ctx.lineTo(px, CY + h / 2 + 4 * S);
                ctx.stroke();
            }

            for (let j = 0; j < 3; j += 1) {
                const py = CY - h / 4 + (j * h) / 4;
                ctx.beginPath();
                ctx.setLineDash([2 * S, 9 * S]);
                ctx.lineDashOffset = -t * 18;
                ctx.strokeStyle = "rgba(155,143,239,0.05)";
                ctx.lineWidth = 1 * S;
                ctx.moveTo(CX - rx + 12 * S, py);
                ctx.lineTo(CX + rx - 12 * S, py);
                ctx.stroke();
            }
            ctx.setLineDash([]);

            for (let i = 0; i < 5; i += 1) {
                const gx = CX + Math.sin(t * 0.45 + i * 1.4) * 28 * S;
                const gy = CY + Math.cos(t * 0.35 + i * 2.1) * 28 * S;
                const ig = ctx.createRadialGradient(gx, gy, 0, gx, gy, 16 * S);
                ig.addColorStop(0, "rgba(91,141,239,0.1)");
                ig.addColorStop(1, "rgba(91,141,239,0)");
                ctx.fillStyle = ig;
                ctx.beginPath();
                ctx.arc(gx, gy, 16 * S, 0, PI2);
                ctx.fill();
            }

            gears.forEach((g) => drawGear(CX + g.x * S, CY + g.y * S, g.oR * S, g.iR * S, g.teeth, t * g.spd, g.col));
            ctx.restore();

            ctx.beginPath();
            ctx.ellipse(CX, CY - h / 2, rx, ry, 0, 0, PI2);
            const tg = ctx.createRadialGradient(CX - rx * 0.18, CY - h / 2 - ry * 0.3, 0, CX, CY - h / 2, rx);
            tg.addColorStop(0, "#ffffff");
            tg.addColorStop(0.55, "#f0f4ff");
            tg.addColorStop(1, "#dce4f5");
            ctx.fillStyle = tg;
            ctx.fill();
            ctx.strokeStyle = "#aec0dc";
            ctx.lineWidth = 1.5 * S;
            ctx.stroke();

            ctx.beginPath();
            ctx.ellipse(CX - rx * 0.12, CY - h / 2 - ry * 0.12, rx * 0.38, ry * 0.28, -0.2, 0, PI2);
            ctx.fillStyle = "rgba(255,255,255,0.35)";
            ctx.fill();
        }

        function drawEntryGlow(t) {
            const p = 0.5 + 0.5 * Math.sin(t * 1.4);
            const g = ctx.createRadialGradient(CX, CY - 58 * S, 0, CX, CY - 58 * S, 42 * S);
            g.addColorStop(0, `rgba(91,141,239,${0.09 * p})`);
            g.addColorStop(1, "rgba(91,141,239,0)");
            ctx.fillStyle = g;
            ctx.beginPath();
            ctx.arc(CX, CY - 58 * S, 42 * S, 0, PI2);
            ctx.fill();
        }

        function drawExitGlow(t) {
            const p = 0.5 + 0.5 * Math.sin(t * 1.2 + 1);
            const g = ctx.createRadialGradient(CX + 78 * S, CY + 12 * S, 0, CX + 78 * S, CY + 12 * S, 38 * S);
            g.addColorStop(0, `rgba(155,143,239,${0.08 * p})`);
            g.addColorStop(1, "rgba(155,143,239,0)");
            ctx.fillStyle = g;
            ctx.beginPath();
            ctx.arc(CX + 78 * S, CY + 12 * S, 38 * S, 0, PI2);
            ctx.fill();
        }

        function drawDoc(x, y, sc, rot, op) {
            ctx.save();
            ctx.translate(x, y);
            ctx.scale(sc, sc);
            ctx.rotate(rot);
            ctx.globalAlpha = op;
            ctx.shadowColor = "rgba(100,130,200,0.14)";
            ctx.shadowBlur = 14 * S;
            ctx.shadowOffsetY = 5 * S;
            rr(-21 * S, -29 * S, 42 * S, 58 * S, 4 * S);
            ctx.fillStyle = "#ffffff";
            ctx.fill();
            ctx.strokeStyle = "#bcc8ee";
            ctx.lineWidth = 1 * S;
            ctx.stroke();
            ctx.shadowColor = "transparent";
            const lw = [24, 29, 17, 25, 13];
            for (let i = 0; i < 5; i += 1) {
                rr((-lw[i] / 2) * S, (-17 + i * 8.5) * S, lw[i] * S, 2.5 * S, 1 * S);
                ctx.fillStyle = i === 0 ? "#94a6d0" : "#d6ddf0";
                ctx.fill();
            }
            ctx.beginPath();
            ctx.moveTo(11 * S, -29 * S);
            ctx.lineTo(21 * S, -19 * S);
            ctx.lineTo(11 * S, -19 * S);
            ctx.closePath();
            ctx.fillStyle = "#edf1ff";
            ctx.fill();
            ctx.strokeStyle = "#bcc8ee";
            ctx.lineWidth = 0.5 * S;
            ctx.stroke();
            ctx.globalAlpha = 1;
            ctx.restore();
        }

        function drawFolder(x, y, sc, rot, op) {
            ctx.save();
            ctx.translate(x, y);
            ctx.scale(sc, sc);
            ctx.rotate(rot);
            ctx.globalAlpha = op;
            ctx.shadowColor = "rgba(100,130,200,0.14)";
            ctx.shadowBlur = 14 * S;
            ctx.shadowOffsetY = 5 * S;
            ctx.beginPath();
            ctx.moveTo(-23 * S, -14 * S);
            ctx.lineTo(-13 * S, -14 * S);
            ctx.lineTo(-9 * S, -21 * S);
            ctx.lineTo(6 * S, -21 * S);
            ctx.lineTo(6 * S, -14 * S);
            ctx.lineTo(23 * S, -14 * S);
            ctx.closePath();
            ctx.fillStyle = "#c4d2ee";
            ctx.fill();
            rr(-23 * S, -14 * S, 46 * S, 32 * S, 3 * S);
            ctx.fillStyle = "#e2eaff";
            ctx.fill();
            ctx.strokeStyle = "#bcc8ee";
            ctx.lineWidth = 1 * S;
            ctx.stroke();
            ctx.shadowColor = "transparent";
            for (let i = 0; i < 2; i += 1) {
                rr(-15 * S, (-5 + i * 7.5) * S, 19 * S, 2 * S, 1 * S);
                ctx.fillStyle = "#c4d0ee";
                ctx.fill();
            }
            rr(5 * S, -9 * S, 13 * S, 19 * S, 2 * S);
            ctx.fillStyle = "#ffffff";
            ctx.fill();
            ctx.strokeStyle = "#bcc8ee";
            ctx.lineWidth = 0.5 * S;
            ctx.stroke();
            ctx.globalAlpha = 1;
            ctx.restore();
        }

        function drawCard(x, y, sc, rot, op, type) {
            ctx.save();
            ctx.translate(x, y);
            ctx.scale(sc, sc);
            ctx.rotate(rot);
            ctx.globalAlpha = op;
            ctx.shadowColor = "rgba(100,130,200,0.14)";
            ctx.shadowBlur = 20 * S;
            ctx.shadowOffsetY = 7 * S;
            rr(-35 * S, -27 * S, 70 * S, 54 * S, 6 * S);
            ctx.fillStyle = "#ffffff";
            ctx.fill();
            ctx.strokeStyle = "#c6d2f0";
            ctx.lineWidth = 1 * S;
            ctx.stroke();
            ctx.shadowColor = "transparent";

            if (type === "bar") {
                const bars = [
                    { h: 15, c: "#5B8DEF" },
                    { h: 22, c: "#5B8DEF" },
                    { h: 10, c: "#9B8FEF" },
                    { h: 18, c: "#5BC4CF" },
                    { h: 7, c: "#9B8FEF" },
                ];
                bars.forEach((b, i) => {
                    rr((-25 + i * 10.5) * S, (13 - b.h) * S, 7.5 * S, b.h * S, 2 * S);
                    ctx.fillStyle = b.c;
                    ctx.globalAlpha = op * (0.35 + (b.h / 22) * 0.65);
                    ctx.fill();
                });
                ctx.globalAlpha = op * 0.3;
                rr(-25 * S, -19 * S, 17 * S, 2 * S, 1 * S);
                ctx.fillStyle = "#a4b4d4";
                ctx.fill();
                rr(-25 * S, -13 * S, 30 * S, 2 * S, 1 * S);
                ctx.fill();
            } else if (type === "line") {
                ctx.globalAlpha = op * 0.8;
                ctx.beginPath();
                ctx.moveTo(-27 * S, 10 * S);
                ctx.lineTo(-15 * S, -3 * S);
                ctx.lineTo(-3 * S, 5 * S);
                ctx.lineTo(9 * S, -9 * S);
                ctx.lineTo(21 * S, -1 * S);
                ctx.lineTo(29 * S, -6 * S);
                ctx.strokeStyle = "#5B8DEF";
                ctx.lineWidth = 2 * S;
                ctx.lineCap = "round";
                ctx.lineJoin = "round";
                ctx.stroke();
                ctx.lineTo(29 * S, 13 * S);
                ctx.lineTo(-27 * S, 13 * S);
                ctx.closePath();
                ctx.fillStyle = "rgba(91,141,239,0.05)";
                ctx.fill();
                [[-27, 10], [-15, -3], [9, -9], [29, -6]].forEach(([px, py]) => {
                    ctx.beginPath();
                    ctx.arc(px * S, py * S, 2.5 * S, 0, PI2);
                    ctx.fillStyle = "#5B8DEF";
                    ctx.globalAlpha = op;
                    ctx.fill();
                });
                ctx.globalAlpha = op * 0.3;
                rr(-27 * S, -19 * S, 15 * S, 2 * S, 1 * S);
                ctx.fillStyle = "#a4b4d4";
                ctx.fill();
            } else if (type === "pie") {
                [
                    { s: 0, e: 1.9, c: "#5B8DEF" },
                    { s: 1.9, e: 3.5, c: "#9B8FEF" },
                    { s: 3.5, e: 5.1, c: "#5BC4CF" },
                    { s: 5.1, e: PI2, c: "#e8eeff" },
                ].forEach((seg) => {
                    ctx.beginPath();
                    ctx.moveTo(0, 2 * S);
                    ctx.arc(0, 2 * S, 13 * S, seg.s, seg.e);
                    ctx.closePath();
                    ctx.fillStyle = seg.c;
                    ctx.globalAlpha = op * 0.6;
                    ctx.fill();
                });
                ctx.beginPath();
                ctx.arc(0, 2 * S, 6 * S, 0, PI2);
                ctx.fillStyle = "#ffffff";
                ctx.globalAlpha = op;
                ctx.fill();
            } else {
                ctx.globalAlpha = op * 0.28;
                for (let i = 0; i < 3; i += 1) {
                    rr(-25 * S, (-17 + i * 7) * S, (34 + (i === 0 ? 8 : 0)) * S, 2 * S, 1 * S);
                    ctx.fillStyle = "#b4c4e0";
                    ctx.fill();
                }
                ctx.globalAlpha = op;
                rr(-25 * S, 5 * S, 9 * S, 9 * S, 2 * S);
                ctx.fillStyle = "rgba(107,203,139,0.28)";
                ctx.fill();
                rr(-13 * S, 5 * S, 9 * S, 9 * S, 2 * S);
                ctx.fillStyle = "rgba(91,141,239,0.28)";
                ctx.fill();
                rr(-1 * S, 5 * S, 9 * S, 9 * S, 2 * S);
                ctx.fillStyle = "rgba(155,143,239,0.28)";
                ctx.fill();
            }
            ctx.globalAlpha = 1;
            ctx.restore();
        }

        function drawDocs(t) {
            docs.forEach((d) => {
                const phase = (d.ph + t * d.spd) % 1;
                let op = 1;
                if (phase < 0.12) op = phase / 0.12;
                else if (phase > 0.82) op = (1 - phase) / 0.18;
                op = clamp(op, 0, 1);
                const et = easeIO(phase);
                const p = bz(et, d.path.p0, d.path.p1, d.path.p2, d.path.p3);
                const x = CX + p.x * S;
                const y = CY + p.y * S;
                const rot = d.rot * Math.sin(t * 0.28 + d.ph * 10);
                const sc = d.sc * (0.78 + 0.22 * (1 - Math.abs(phase - 0.5) * 2));
                if (d.type === "folder") drawFolder(x, y, sc, rot, op * 0.88);
                else drawDoc(x, y, sc, rot, op);
            });
        }

        function drawCards(t) {
            cards.forEach((c) => {
                const phase = (c.ph + t * c.spd) % 1;
                let op = 1;
                if (phase < 0.12) op = phase / 0.12;
                else if (phase > 0.78) op = (1 - phase) / 0.22;
                op = clamp(op, 0, 1);
                const et = easeIO(phase);
                const p = bz(et, c.path.p0, c.path.p1, c.path.p2, c.path.p3);
                const x = CX + p.x * S;
                const y = CY + p.y * S;
                const rot = c.rot * Math.sin(t * 0.22 + c.ph * 8);
                const sc = c.sc * (0.72 + 0.28 * (1 - Math.abs(phase - 0.5) * 2));
                drawCard(x, y, sc, rot, op * 0.92, c.type);
            });
        }

        function drawTransformZone(t) {
            const pulse = 0.5 + 0.5 * Math.sin(t * 0.8);
            const g = ctx.createLinearGradient(CX - 90 * S, CY, CX + 90 * S, CY);
            g.addColorStop(0, "rgba(91,141,239,0)");
            g.addColorStop(0.3, `rgba(91,141,239,${0.025 * pulse})`);
            g.addColorStop(0.5, `rgba(155,143,239,${0.04 * pulse})`);
            g.addColorStop(0.7, `rgba(91,141,239,${0.025 * pulse})`);
            g.addColorStop(1, "rgba(91,141,239,0)");
            ctx.fillStyle = g;
            ctx.fillRect(CX - 90 * S, CY - 3 * S, 180 * S, 6 * S);
        }

        function drawFrame(ts) {
            const t = ts * 0.001;
            ctx.clearRect(0, 0, W, H);
            drawBg();
            drawBgDots(t);
            drawPaths(t);
            drawParticles(t);
            drawEntryGlow(t);
            drawDocs(t);
            drawTransformZone(t);
            drawDatabase(t);
            drawExitGlow(t);
            drawCards(t);
            rafId = window.requestAnimationFrame(drawFrame);
        }

        resize();

        const reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
        if (reduceMotion) {
            drawBg();
            drawDatabase(0);
        } else {
            rafId = window.requestAnimationFrame(drawFrame);
        }

        let observer;
        if (typeof ResizeObserver !== "undefined" && wrapRef.current) {
            observer = new ResizeObserver(() => resize());
            observer.observe(wrapRef.current);
        }

        window.addEventListener("resize", resize);

        return () => {
            window.cancelAnimationFrame(rafId);
            window.removeEventListener("resize", resize);
            if (observer) observer.disconnect();
        };
    }, []);

    return (
        <div ref={wrapRef} className="auth-canvas-wrap" aria-hidden="true">
            <canvas ref={canvasRef} className="auth-canvas-bg" />
            <div className="auth-canvas-overlay" />
        </div>
    );
}
